use crate::{PAGE_SIZE, PageId, SlotId};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Slot {
    offset: u16,
    length: u16,
}

pub struct SlottedPage {
    pub data: [u8; PAGE_SIZE],
}

impl SlottedPage {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_slot_count(0);
        page.set_free_space_pointer(PAGE_SIZE as u16);
        page
    }

    #[allow(unused)]
    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    fn set_page_id(&mut self, id: PageId) {
        self.data[0..4].copy_from_slice(&id.to_le_bytes());
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[4..6].try_into().unwrap())
    }

    fn set_slot_count(&mut self, count: u16) {
        self.data[4..6].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_space_pointer(&self) -> u16 {
        u16::from_le_bytes(self.data[6..8].try_into().unwrap())
    }

    fn set_free_space_pointer(&mut self, ptr: u16) {
        self.data[6..8].copy_from_slice(&ptr.to_le_bytes());
    }

    fn get_slot(&self, slot_id: SlotId) -> Option<Slot> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let base = 8 + (slot_id as usize) * 4;
        let offset = u16::from_le_bytes(self.data[base..base + 2].try_into().unwrap());
        let length = u16::from_le_bytes(self.data[base + 2..base + 4].try_into().unwrap());
        Some(Slot { offset, length })
    }

    fn set_slot(&mut self, slot_id: SlotId, slot: Slot) {
        let base = 8 + (slot_id as usize) * 4;
        self.data[base..base + 2].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[base + 2..base + 4].copy_from_slice(&slot.length.to_le_bytes());
    }

    pub fn available_space(&self) -> usize {
        let header_end = 8 + (self.slot_count() as usize) * 4;
        let free_ptr = self.free_space_pointer() as usize;
        if free_ptr < header_end + 4 {
            0
        } else {
            free_ptr - header_end - 4
        }
    }

    pub fn insert(&mut self, record_bytes: &[u8]) -> Option<SlotId> {
        let needed = record_bytes.len() + 4;
        if self.available_space() < needed {
            return None;
        }

        let new_free_ptr = self.free_space_pointer() - record_bytes.len() as u16;
        self.set_free_space_pointer(new_free_ptr);

        let offset = new_free_ptr;
        self.data[offset as usize..offset as usize + record_bytes.len()]
            .copy_from_slice(record_bytes);

        let slot_id = self.slot_count();
        self.set_slot(
            slot_id,
            Slot {
                offset,
                length: record_bytes.len() as u16,
            },
        );
        self.set_slot_count(slot_id + 1);

        Some(slot_id)
    }

    pub fn get(&self, slot_id: SlotId) -> Option<&[u8]> {
        let slot = self.get_slot(slot_id)?;
        if slot.length == 0 {
            return None;
        }
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        Some(&self.data[start..end])
    }

    pub fn update(&mut self, slot_id: SlotId, new_bytes: &[u8]) -> bool {
        let slot = match self.get_slot(slot_id) {
            Some(s) if s.length > 0 => s,
            _ => return false,
        };

        if new_bytes.len() <= slot.length as usize {
            let start = slot.offset as usize;
            self.data[start..start + new_bytes.len()].copy_from_slice(new_bytes);
            self.set_slot(
                slot_id,
                Slot {
                    offset: slot.offset,
                    length: new_bytes.len() as u16,
                },
            );
            true
        } else {
            false
        }
    }

    pub fn delete(&mut self, slot_id: SlotId) -> bool {
        if let Some(slot) = self.get_slot(slot_id) {
            if slot.length > 0 {
                self.set_slot(
                    slot_id,
                    Slot {
                        offset: 0,
                        length: 0,
                    },
                );
                return true;
            }
        }
        false
    }
}
