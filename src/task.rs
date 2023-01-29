pub enum Task<T: Send> {
    NewTask(T, usize),
    Dropped(usize),
    Terminate(usize),
}
