use core::{
	cell::{Cell, RefCell},
	cmp,
	ffi::CStr,
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

use crate::{
	future::{FutureOrOutput, SliceJoin},
	FileGroup,
	Globals,
};

pub async fn hash_group(
	globals: &Globals,
	mut group: FileGroup,
	new_groups: &RefCell<Vec<FileGroup>>,
	fd_semaphore: &Semaphore,
	completion_count: &Cell<usize>,
	range: Range<u64>,
) {
	if group.info.size < range.start {
		new_groups.borrow_mut().push(group);
		return;
	}

	let mut files: Vec<_> = group
		.files
		.iter_mut()
		.map(|file| {
			FutureOrOutput::Future(async move {
				const READ_CHUNK: usize = 2 * 1024 * 1024;

				let _guard = fd_semaphore.wait().await;
				let target_size = cmp::min(group.info.size, range.end);

				let new_completion_count = completion_count.get() + 1;
				completion_count.set(new_completion_count);

				let mut path_buf = Vec::new();
				file.path[0].flatten(&mut path_buf);
				path_buf.push(0);

				let fd = match Openat2::new(
					&globals.ring,
					CWD,
					CStr::from_bytes_until_nul(&path_buf).unwrap(),
					&globals.file_open_how,
				)
				.await
				{
					Ok(fd) => fd,
					Err(err) => {
						file.ino = 0;
						eprintln!("Error opening {}: {}", file.path[0], err);
						return;
					}
				};

				// This will add it to the sq but *not* submit. We want to submit *with* the next read. Cleanup will happen later.
				// Don't bother doing this if we're gonna immediately read the whole thing.
				let fadvise = if range.end - range.start > READ_CHUNK as u64 {
					Some(
						Fadvise::new(
							&globals.ring,
							fd.as_fd(),
							range.start,
							(range.end - range.start).try_into().unwrap_or(u32::MAX),
							Advice::WillNeed,
						)
						.link(),
					)
				} else {
					None
				};

				let mut buffer: Box<[u8]> =
					vec![0; cmp::min(target_size.try_into().unwrap(), READ_CHUNK,)].into();

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
							if total_read == target_size {
								break;
							} else {
								eprintln!(
									"File size changed {}: {} -> {}",
									file.path[0], target_size, total_read
								);
								return;
							}
						}
						Ok(x) => {
							total_read += <u32 as Into<u64>>::into(x);
							assert!(total_read <= target_size);
							hash.write(&buffer[0..(x.try_into().unwrap())]);
						}
						Err(Errno::AGAIN | Errno::INTR | Errno::CANCELED) => (),
						Err(err) => {
							file.ino = 0;
							eprintln!("Error reading {}: {}", file.path[0], err);
							return;
						}
					}
				}

				// Cleanup fadvise from earlier (it doesn't have a good drop impl yet).
				if let Some(f) = fadvise {
					let _ = f.await;
				}

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

	SliceJoin::new(&mut files).await;
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
