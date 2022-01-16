use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::Vec;

use futures::future::FusedFuture;

pub trait Iterable {
    type Item;
    type Iter: Iterator<Item = Self::Item>;

    fn iter(&self) -> Self::Iter;
}

impl<'a, T> Iterable for &'a [T] {
    type Item = &'a T;
    type Iter = std::slice::Iter<'a, T>;

    fn iter(&self) -> std::slice::Iter<'a, T> {
        (*self).iter()
    }
}

pub trait MutIterable<'a> {
    type Item: 'a;
    type Iter: Iterator<Item = &'a mut Self::Item>;

    fn iter_mut(&'a mut self) -> Self::Iter;
}

impl<'a, T: 'a> MutIterable<'a> for Vec<T> {
    type Item = T;
    type Iter = std::slice::IterMut<'a, T>;

    fn iter_mut(&'a mut self) -> Self::Iter {
        self.as_mut_slice().iter_mut()
    }
}

impl<'a, T: 'a> MutIterable<'a> for VecDeque<T> {
    type Item = T;
    type Iter = std::collections::vec_deque::IterMut<'a, T>;

    fn iter_mut(&'a mut self) -> Self::Iter {
        self.iter_mut()
    }
}

pub struct SelectIterable<'a, F: FusedFuture + 'a, Collection>
where
    Collection: MutIterable<'a, Item = F>, {
    items: &'a mut Collection,
}

impl<'a, F: FusedFuture + 'a, Collection: MutIterable<'a, Item = F>> SelectIterable<'a, F, Collection> {
    pub async fn next(iterable: &'a mut Collection) -> (usize, F::Output) {
        let future = SelectIterable { items: iterable };
        future.await
    }

    pub fn new(iterable: &'a mut Collection) -> SelectIterable<'a, F, Collection> {
        SelectIterable { items: iterable }
    }
}

impl<'a, F: FusedFuture + 'a, Collection: MutIterable<'a, Item = F>> Future for SelectIterable<'a, F, Collection> {
    type Output = (usize, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let items = unsafe { std::mem::transmute::<&'_ mut Collection, &'a mut Collection>(self.items) };
        let iter = items.iter_mut();
        let item = iter.enumerate().find_map(|(i, f)| {
            let pinned = unsafe { Pin::new_unchecked(f) };
            match pinned.poll(cx) {
                Poll::Pending => None,
                Poll::Ready(r) => Some((i, r)),
            }
        });
        match item {
            None => Poll::Pending,
            Some(r) => Poll::Ready(r),
        }
    }
}

struct SelectNext<'a, F> {
    futures: Pin<&'a mut [F]>,
}

impl<'a, F: FusedFuture> Future for SelectNext<'a, F> {
    type Output = (usize, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let futures = unsafe { self.futures.as_mut().get_unchecked_mut() };
        let item = futures.iter_mut().enumerate().find_map(|(i, f)| {
            if f.is_terminated() {
                return None;
            }
            let pinned = unsafe { Pin::new_unchecked(f) };
            match pinned.poll(cx) {
                Poll::Pending => None,
                Poll::Ready(r) => Some((i, r)),
            }
        });
        match item {
            None => Poll::Pending,
            Some(r) => Poll::Ready(r),
        }
    }
}

pub struct SelectAll<'a, F> {
    futures: Pin<&'a mut [F]>,
}

impl<'a, F: FusedFuture> SelectAll<'a, F> {
    pub async fn next(&mut self) -> (usize, F::Output) {
        let next = SelectNext { futures: self.futures.as_mut() };
        return next.await;
    }

    pub fn new(futures: &'a mut [F]) -> SelectAll<'a, F> {
        SelectAll { futures: unsafe { Pin::new_unchecked(futures) } }
    }

    pub fn is_terminated(&self) -> bool {
        return self.futures.iter().all(|f| f.is_terminated());
    }
}
