//! Colas FIFO, prioridad de codificación y afinidad de worker.

use crate::types::WorkType;
use std::collections::VecDeque;
use uuid::Uuid;

pub const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct Task {
    /// UUID estable generado por el head; se conserva entre reintentos.
    pub id: Uuid,
    pub filename: String,
    pub work_type: WorkType,
    pub arguments: Vec<String>,
    /// Intentos consumidos (fallos de proceso o de aceptación).
    pub attempts: u32,
    /// Índice del worker preferido (p. ej. tras una respuesta ambigua o tras
    /// una búsqueda de CRF cuya carga ya está en ese worker).
    pub affinity: Option<usize>,
}

impl Task {
    pub fn new(filename: String, work_type: WorkType, arguments: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            filename,
            work_type,
            arguments,
            attempts: 0,
            affinity: None,
        }
    }
}

/// Cola A: búsquedas de CRF. Cola B: codificaciones (prioritarias).
#[derive(Default)]
pub struct Queues {
    pub crf: VecDeque<Task>,
    pub encode: VecDeque<Task>,
}

impl Queues {
    /// Extrae la siguiente tarea planificable para `worker`:
    /// primero Encode, después CrfSearch; primero con afinidad a este worker,
    /// después sin afinidad.
    pub fn pop_for(&mut self, worker: usize) -> Option<Task> {
        pop_from(&mut self.encode, worker).or_else(|| pop_from(&mut self.crf, worker))
    }

    pub fn requeue_front(&mut self, task: Task) {
        match task.work_type {
            WorkType::Encode => self.encode.push_front(task),
            WorkType::CrfSearch => self.crf.push_front(task),
        }
    }

    pub fn remove(&mut self, id: &Uuid) -> Option<Task> {
        for q in [&mut self.encode, &mut self.crf] {
            if let Some(pos) = q.iter().position(|t| &t.id == id) {
                return q.remove(pos);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.crf.is_empty() && self.encode.is_empty()
    }
}

fn pop_from(q: &mut VecDeque<Task>, worker: usize) -> Option<Task> {
    let pos = q
        .iter()
        .position(|t| t.affinity == Some(worker))
        .or_else(|| q.iter().position(|t| t.affinity.is_none()))?;
    q.remove(pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(ty: WorkType, affinity: Option<usize>) -> Task {
        let mut t = Task::new("v.mkv".into(), ty, vec![]);
        t.affinity = affinity;
        t
    }

    #[test]
    fn encode_has_priority_over_crf() {
        let mut q = Queues::default();
        q.crf.push_back(task(WorkType::CrfSearch, None));
        q.encode.push_back(task(WorkType::Encode, None));
        assert_eq!(q.pop_for(0).unwrap().work_type, WorkType::Encode);
        assert_eq!(q.pop_for(0).unwrap().work_type, WorkType::CrfSearch);
    }

    #[test]
    fn affinity_is_respected() {
        let mut q = Queues::default();
        q.encode.push_back(task(WorkType::Encode, Some(1)));
        q.encode.push_back(task(WorkType::Encode, None));
        // Worker 0 solo puede tomar la tarea sin afinidad.
        assert!(q.pop_for(0).is_some());
        assert!(q.pop_for(0).is_none());
        // Worker 1 toma la suya.
        assert_eq!(q.pop_for(1).unwrap().affinity, Some(1));
    }
}
