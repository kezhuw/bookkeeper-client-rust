// Task local rc. Don't send it across tasks/threads.
#[repr(transparent)]
pub(crate) struct LocalRc<T>(std::rc::Rc<T>);

impl<T> LocalRc<T> {
    pub fn new(v: T) -> LocalRc<T> {
        LocalRc(std::rc::Rc::new(v))
    }
}

impl<T> std::ops::Deref for LocalRc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for LocalRc<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> Clone for LocalRc<T> {
    fn clone(&self) -> Self {
        LocalRc(self.0.clone())
    }
}

unsafe impl<T: Send> Send for LocalRc<T> {}
