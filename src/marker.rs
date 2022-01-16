use std::marker::PhantomData;

use static_assertions::{assert_impl_all, assert_not_impl_any};

pub(crate) struct Sendable(std::rc::Rc<()>);

unsafe impl Send for Sendable {}
assert_impl_all!(Sendable: Send);
assert_not_impl_any!(Sendable: Sync);

assert_impl_all!(PhantomData<Sendable>: Send);
assert_not_impl_any!(PhantomData<Sendable>: Sync);
