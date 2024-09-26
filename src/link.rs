use core::{ffi::CStr, fmt::Write};

use rustix::fs::{linkat, renameat_with, unlink, AtFlags, RenameFlags, CWD};

use crate::{FileGroup, Globals};

const TMP_FILE_NAME: &[u8] = b"DUPELINK_TMP_FILE";

pub fn link(
	globals: &Globals,
	groups: Vec<FileGroup>,
	total_deduped_files: &mut u64,
	total_deduped_size: &mut u64,
) {
	let mut buffer = String::new();
	let mut path_buf = Vec::new();
	for mut group in groups {
		buffer.clear();

		let nr_deduped_files: u64 = (group.files.len() - 1).try_into().unwrap();
		*total_deduped_files += nr_deduped_files;
		let deduped_size = nr_deduped_files * group.info.size;
		*total_deduped_size += deduped_size;

		writeln!(
			&mut buffer,
			"{} bytes each, {} bytes deduped",
			group.info.size, deduped_size,
		)
		.unwrap();

		// Sort by ctime
		group.files.sort_unstable_by(|a, b| a.ctime.cmp(&b.ctime));

		let mut names = group.files.iter().flat_map(|file| file.path.iter());
		let master_name = names.next().unwrap();

		writeln!(&mut buffer, "{:?}", master_name).unwrap();

		for name in names {
			// TODO, check that file hasn't changed.
			if globals.link {
				name.to_bytes()
					.rsplit_once(|c| *c == b'/')
					.unwrap()
					.0
					.clone_into(&mut path_buf);
				path_buf.push(b'/');
				path_buf.extend_from_slice(TMP_FILE_NAME);
				path_buf.push(0);

				let tmp_name = CStr::from_bytes_with_nul(&path_buf).unwrap();

				if let Err(err) = linkat(CWD, master_name, CWD, tmp_name, AtFlags::empty()) {
					eprintln!("Error linking {:?} to {:?}: {}", master_name, tmp_name, err);
					continue;
				}

				let mut error_flag = false;
				if let Err(err) = renameat_with(CWD, name, CWD, tmp_name, RenameFlags::EXCHANGE) {
					eprintln!("Error exchanging {:?} and {:?}: {}", name, tmp_name, err);
					// Explicitly no `continue` statement, we want the unlink operation to apply unconditionally
					error_flag = true;
				}

				if let Err(err) = unlink(tmp_name) {
					eprintln!("Error unlinking {:?}: {}", tmp_name, err);
					continue;
				}

				if error_flag {
					continue;
				}
			}
			writeln!(&mut buffer, "{:?}", name).unwrap();
		}

		println!("{}", buffer);
	}
}
