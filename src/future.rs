use core::{
	cell::RefCell,
	future::Future,
	iter::Fuse,
	pin::Pin,
	task::{Context, Poll, Waker},
};

use uring_async::Uring;

pub fn block_on<F: Future>(ring: &RefCell<Uring>, pre_block: &dyn Fn(), mut fut: F) -> F::Output {
	loop {
		if let Poll::Ready(x) = Future::poll(
			unsafe { Pin::new_unchecked(&mut fut) },
			&mut Context::from_waker(Waker::noop()),
		) {
			break x;
		}

		let mut borrowed_ring = ring.borrow_mut();
		let in_flight = borrowed_ring.in_flight();

		if borrowed_ring.sq_enqueued() != 0 || in_flight != 0 {
			pre_block();
			borrowed_ring.submit(in_flight / 8 + 1);
		}
	}
}

pub enum FutureOrOutput<F, O> {
	Future(F),
	Output(O),
}

impl<F, O> FutureOrOutput<F, O> {
	pub fn unwrap_output(self) -> O {
		match self {
			FutureOrOutput::Output(x) => x,
			_ => unreachable!(),
		}
	}
}

pub struct SliceJoin<'a, F: Future>(&'a mut [FutureOrOutput<F, <F as Future>::Output>]);

impl<'a, F: Future> SliceJoin<'a, F> {
	pub fn new(inner: &'a mut [FutureOrOutput<F, <F as Future>::Output>]) -> Self {
		Self(inner)
	}
}

impl<'a, F: Future> Future for SliceJoin<'a, F> {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = Pin::into_inner(self);
		let mut output_count = 0;
		for fut_or_output in this.0.iter_mut() {
			match fut_or_output {
				FutureOrOutput::Output(_) => {
					output_count += 1;
				}
				FutureOrOutput::Future(f) => {
					if let Poll::Ready(output) = Future::poll(unsafe { Pin::new_unchecked(f) }, cx)
					{
						*fut_or_output = FutureOrOutput::Output(output);
						output_count += 1;
					}
				}
			}
		}

		if output_count == this.0.len() {
			Poll::Ready(())
		} else {
			Poll::Pending
		}
	}
}

pub struct IteratorJoin<F: Future<Output = ()>, I: Iterator<Item = F>> {
	buffer: Vec<Option<F>>,
	iter: Fuse<I>,
}

impl<F: Future<Output = ()>, I: Iterator<Item = F>> IteratorJoin<F, I> {
	pub fn new(n: usize, iter: I) -> Self {
		Self {
			buffer: (0..n).map(|_| None).collect(),
			iter: iter.fuse(),
		}
	}
}

impl<F: Future<Output = ()>, I: Iterator<Item = F>> Future for IteratorJoin<F, I> {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = unsafe { Pin::into_inner_unchecked(self) };

		let mut any_pending = false;
		for slot in this.buffer.iter_mut() {
			loop {
				if let Some(f) = slot {
					if Future::poll(unsafe { Pin::new_unchecked(f) }, cx).is_ready() {
						*slot = None;
					} else {
						any_pending = true;
						break;
					}
				} else if let Some(f) = this.iter.next() {
					*slot = Some(f);
				} else {
					break;
				}
			}
		}

		if any_pending {
			Poll::Pending
		} else {
			Poll::Ready(())
		}
	}
}
