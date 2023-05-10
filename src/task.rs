pub enum Task<T: Send> {
    NewTask(T),
    Dropped,
    Terminate,
}

pub struct Message<T: Send> {
    pub op: Task<T>,
    pub order: usize,
}
impl<T: Send> Message<T> {
    pub fn new(op: Task<T>, order: usize) -> Message<T> {
        Message { op, order }
    }

    pub fn is_terminate(&self) -> bool {
        matches!(self.op, Task::Terminate)
    }
}
