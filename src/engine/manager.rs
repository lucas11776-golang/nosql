use std::{
    collections::{HashMap, VecDeque},
    fs::OpenOptions,
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};

use anyhow::{Result, anyhow};
use tokio::sync::{Mutex, OwnedRwLockWriteGuard, RwLock};

use crate::{PAGE_SIZE, PageId, engine::slotted::SlottedPage};

pub struct DiskManager {
    pub(crate) file: std::fs::File,
    pub(crate) num_pages: Mutex<PageId>,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let metadata = file.metadata()?;
        let num_pages = (metadata.len() / PAGE_SIZE as u64) as PageId;
        Ok(Self {
            file,
            num_pages: Mutex::new(num_pages),
        })
    }

    pub async fn allocate_page(&self) -> Result<PageId> {
        let mut guard = self.num_pages.lock().await;
        let page_id = *guard;
        *guard += 1;
        let empty_page = SlottedPage::new(page_id);
        self.write_page(page_id, &empty_page.data)?;
        Ok(page_id)
    }

    pub fn read_page(&self, page_id: PageId, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }

    pub fn write_page(&self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.write_all_at(buf, offset)?;
        Ok(())
    }
}

struct Frame {
    page_id: Option<PageId>,
    pin_count: u32,
    is_dirty: bool,
    data: SlottedPage,
}

pub struct BufferPoolManager {
    disk_manager: Arc<DiskManager>,
    frames: Vec<Arc<RwLock<Frame>>>,
    page_table: Mutex<HashMap<PageId, usize>>,
    free_list: Mutex<VecDeque<usize>>,
}

impl BufferPoolManager {
    pub fn new(capacity: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(capacity);
        let mut free_list = VecDeque::with_capacity(capacity);
        for i in 0..capacity {
            frames.push(Arc::new(RwLock::new(Frame {
                page_id: None,
                pin_count: 0,
                is_dirty: false,
                data: SlottedPage::new(0),
            })));
            free_list.push_back(i);
        }
        Self {
            disk_manager,
            frames,
            page_table: Mutex::new(HashMap::new()),
            free_list: Mutex::new(free_list),
        }
    }

    pub async fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> Result<PageWriteGuard> {
        let mut pt = self.page_table.lock().await;

        if let Some(&frame_id) = pt.get(&page_id) {
            let mut frame_guard = self.frames[frame_id].clone().write_owned().await;
            frame_guard.pin_count += 1;
            return Ok(PageWriteGuard { frame_guard });
        }

        let frame_id = if let Some(fid) = self.free_list.lock().await.pop_front() {
            fid
        } else {
            let mut evicted_fid = None;
            for (i, frame_lock) in self.frames.iter().enumerate() {
                let f = frame_lock.read().await;
                if f.pin_count == 0 {
                    evicted_fid = Some((i, f.page_id, f.is_dirty));
                    break;
                }
            }

            let (fid, old_pid, is_dirty) =
                evicted_fid.ok_or_else(|| anyhow!("Buffer pool full: all pages are pinned"))?;

            if is_dirty {
                if let Some(opid) = old_pid {
                    let f = self.frames[fid].read().await;
                    self.disk_manager.write_page(opid, &f.data.data)?;
                }
            }

            if let Some(opid) = old_pid {
                pt.remove(&opid);
            }
            fid
        };

        let mut page_data = SlottedPage::new(page_id);
        self.disk_manager.read_page(page_id, &mut page_data.data)?;

        let mut frame_guard = self.frames[frame_id].clone().write_owned().await;
        frame_guard.page_id = Some(page_id);
        frame_guard.pin_count = 1;
        frame_guard.is_dirty = false;
        frame_guard.data = page_data;

        pt.insert(page_id, frame_id);

        Ok(PageWriteGuard { frame_guard })
    }

    pub async fn flush_all(&self) -> Result<()> {
        let pt = self.page_table.lock().await;
        for (&pid, &fid) in pt.iter() {
            let mut f = self.frames[fid].write().await;
            if f.is_dirty {
                self.disk_manager.write_page(pid, &f.data.data)?;
                f.is_dirty = false;
            }
        }
        Ok(())
    }
}

pub struct PageWriteGuard {
    frame_guard: OwnedRwLockWriteGuard<Frame>,
}

impl PageWriteGuard {
    pub fn mark_dirty(&mut self) {
        self.frame_guard.is_dirty = true;
    }
}

impl std::ops::Deref for PageWriteGuard {
    type Target = SlottedPage;
    fn deref(&self) -> &Self::Target {
        &self.frame_guard.data
    }
}

impl std::ops::DerefMut for PageWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame_guard.data
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        if self.frame_guard.pin_count > 0 {
            self.frame_guard.pin_count -= 1;
        }
    }
}
