// pub use std::cell::Cell;
// pub use std::cell::RefCell;

pub struct Cell<T: ?Sized>(std::cell::Cell<T>);
pub struct Ref<'b, T: ?Sized + 'b>(std::cell::Ref<'b, T>);

impl<T> Cell<T> {
    pub fn new(val: T) -> Self {
        Cell(std::cell::Cell::new(val))
    }

    pub fn set(&self, val: T) {
        self.0.set(val)
    }

    pub fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }
}

impl<T: Default> Cell<T> {
    pub fn take(&self) -> T {
        self.0.take()
    }
}

impl<T: Copy> Cell<T> {
    pub fn get(&self) -> T {
        self.0.get()
    }
}

impl<T: Clone> Clone for Cell<T> {
    fn clone(&self) -> Self {
        let value = unsafe { &*self.0.as_ptr() };
        Cell(std::cell::Cell::new(value.clone()))
    }
}

pub struct RefCell<T: ?Sized>(std::cell::RefCell<T>);

impl<T> RefCell<T> {
    pub fn new(val: T) -> Self {
        RefCell(std::cell::RefCell::new(val))
    }

    pub fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self.0.borrow())
    }

    pub fn try_borrow_mut(&self) -> Result<std::cell::RefMut<'_, T>, std::cell::BorrowMutError> {
        self.0.try_borrow_mut()
    }
}

impl<T: Clone> Clone for RefCell<T> {
    fn clone(&self) -> Self {
        RefCell(self.0.clone())
    }
}

impl<T: ?Sized> std::ops::Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

unsafe impl<T: Send> Send for Cell<T> {}
unsafe impl<T: Send> Sync for Cell<T> {}

unsafe impl<T: Send> Send for Ref<'_, T> {}
unsafe impl<T: Send> Sync for Ref<'_, T> {}

unsafe impl<T: Send> Send for RefCell<T> {}
unsafe impl<T: Sync> Sync for RefCell<T> {}
