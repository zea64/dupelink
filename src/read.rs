use core::{
	cell::{Cell, RefCell},
	cmp,
	hash::Hasher,
	ops::Range,
};

use rustix::{
	fd::AsFd,
	fs::{Advice, CWD},
	io::Errno,
};
use uring_async::{
	ops::{Close, Fadvise, Openat2, Read, UringOp},
	sync::Semaphore,
};

use crate::{FileGroup, FutureOrOutput, Globals, SliceJoin};

pub async fn hash_group(
	globals: &Globals,
	mut group: FileGroup,
	new_groups: &RefCell<Vec<FileGroup>>,
	fd_semaphore: &Semaphore,
	completion_count: &Cell<usize>,
	total_count: usize,
	range: Range<u64>,
) {
	if group.info.size < range.start {
		new_groups.borrow_mut().push(group);
		return;
	}

	let mut files: Vec<_> =
		group
		.files
		.iter_mut()
		.map(|file| {
			FutureOrOutput::Future(async move {
				let _guard = fd_semaphore.wait().await;
				let target_size = cmp::min(group.info.size, range.end);

				let new_completion_count = completion_count.get() + 1;
				completion_count.set(new_completion_count);

				let integer_part = new_completion_count * 100 / total_count;
				let frac_part = new_completion_count * 10000 / total_count - integer_part * 100;

				eprint!("{}", format!("\r[{new_completion_count}/{total_count}] ({integer_part:.2}.{frac_part:.2}%)"));

				let fd = match Openat2::new(
					&globals.ring,
					CWD,
					file.path.first_name(),
					&globals.file_open_how,
				)
				.await
				{
					Ok(fd) => fd,
					Err(err) => {
						file.ino = 0;
						eprintln!("Error opening {:?}: {}", file.path.first_name(), err);
						return;
					}
				};

				// This will add it to the sq but *not* submit. We want to submit *with* the next read. Cleanup will happen later.
				let fadvise = Fadvise::new(
					&globals.ring,
					fd.as_fd(),
					range.start,
					(range.end - range.start).try_into().unwrap_or(u32::MAX),
					Advice::WillNeed,
				)
				.link();

				let mut buffer: Box<[u8]> =
				vec![0; cmp::min(target_size.try_into().unwrap(), 2 * 1024 * 1024,)].into();

				let mut hash = std::hash::DefaultHasher::new();
				let mut total_read: u64 = 0;
				loop {
					let to_read =
					cmp::min((target_size - total_read).try_into().unwrap(), buffer.len());
					let read =
					Read::new(&globals.ring, fd.as_fd(), u64::MAX, &mut buffer[0..to_read])
					.await;

					match read {
						Ok(0) => {
							assert_eq!(total_read, target_size);
							break;
						}
						Ok(x) => {
							total_read += <u32 as Into<u64>>::into(x);
							assert!(total_read <= target_size);
							hash.write(&buffer[0..(x.try_into().unwrap())]);
						}
						Err(Errno::AGAIN | Errno::INTR | Errno::CANCELED) => (),
						Err(err) => {
							file.ino = 0;
							eprintln!("Error reading {:?}: {}", file.path.first_name(), err);
							return;
						}
					}
				}

				// Cleanup fadvise from earlier (it doesn't have a good drop impl yet).
				let _ = fadvise.await;

				let _ = Fadvise::new(
					&globals.ring,
					fd.as_fd(),
					range.start,
					(range.end - range.start).try_into().unwrap_or(u32::MAX),
					Advice::DontNeed,
				)
				.await;

				let _ = Close::new(&globals.ring, fd).await;

				file.hash = hash.finish();
			})
		})
		.collect();

	SliceJoin(&mut files).await;
	drop(files);

	group.files.sort_unstable();

	for chunk in group.files.chunk_by(|a, b| a.hash == b.hash) {
		// Inode 0 signals an error processing that inode.
		if chunk.len() <= 1 || chunk[0].ino == 0 {
			continue;
		}
		new_groups.borrow_mut().push(FileGroup {
			info: group.info,
			files: chunk.to_owned(),
		});
	}
}
