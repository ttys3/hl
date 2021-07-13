use std::cmp::min;

// ---

pub trait Push<T: Clone> {
    fn push(&mut self, value: T);
    fn extend_from_slice(&mut self, values: &[T]) {
        for value in values {
            self.push(value.clone());
        }
    }
}

impl<T: Clone> Push<T> for &mut Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        Vec::extend_from_slice(self, values)
    }
}

impl<T: Clone> Push<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        Vec::extend_from_slice(self, values)
    }
}

// impl<T: Clone, P: Push<T>> Push<T> for &mut P {
//     fn push(&mut self, value: T) {
//         Push::<T>::push(self, value)
//     }

//     fn extend_from_slice(&mut self, values: &[T]) {
//         Push::<T>::extend_from_slice(self, values)
//     }
// }

// ---

pub struct Counter {
    value: usize,
}

impl Counter {
    pub fn new() -> Self {
        Self { value: 0 }
    }

    pub fn result(&self) -> usize {
        self.value
    }
}

impl<T: Clone> Push<T> for Counter {
    fn push(&mut self, _: T) {
        self.value += 1
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        self.value += values.len()
    }
}

// ---

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
pub enum Alignment {
    Left,
    Right,
    Center,
}

// --

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Padding<T> {
    pub pad: T,
    pub width: usize,
}

impl<T> Padding<T> {
    pub fn new(pad: T, width: usize) -> Self {
        Self { pad, width }
    }
}

// ---

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Adjustment<T> {
    pub alignment: Alignment,
    pub padding: Padding<T>,
}

impl<T> Adjustment<T> {
    pub fn new(alignment: Alignment, padding: Padding<T>) -> Self {
        Self { alignment, padding }
    }
}

// ---

pub enum Aligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    Disabled(DisabledAligner<'a, O>),
    Unbuffered(UnbufferedAligner<'a, T, O>),
    Buffered(BufferedAligner<'a, T, O>),
}

impl<'a, T, O> Aligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn new(out: &'a mut O, adjustment: Option<Adjustment<T>>) -> Self {
        if let Some(adjustment) = adjustment {
            match adjustment.alignment {
                Alignment::Left => {
                    Self::Unbuffered(UnbufferedAligner::new(out, adjustment.padding))
                }
                Alignment::Center | Alignment::Right => Self::Buffered(BufferedAligner::new(
                    out,
                    adjustment.padding,
                    adjustment.alignment,
                )),
            }
        } else {
            Self::Disabled(DisabledAligner::new(out))
        }
    }

    fn push(&mut self, value: T) {
        match self {
            Self::Disabled(ref mut aligner) => aligner.push(value),
            Self::Unbuffered(ref mut aligner) => aligner.push(value),
            Self::Buffered(ref mut aligner) => aligner.push(value),
        }
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        match self {
            Self::Disabled(ref mut aligner) => aligner.extend_from_slice(values),
            Self::Unbuffered(ref mut aligner) => aligner.extend_from_slice(values),
            Self::Buffered(ref mut aligner) => aligner.extend_from_slice(values),
        }
    }
}

impl<'a, T, B> Push<T> for Aligner<'a, T, B>
where
    T: Clone,
    B: Push<T>,
{
    fn push(&mut self, value: T) {
        Aligner::push(self, value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        Aligner::extend_from_slice(self, values)
    }
}

// ---

pub struct DisabledAligner<'a, O> {
    out: &'a mut O,
}

impl<'a, O> DisabledAligner<'a, O> {
    pub fn new(out: &'a mut O) -> Self {
        Self { out }
    }
}

impl<'a, T, O> Push<T> for DisabledAligner<'a, O>
where
    T: Clone,
    O: Push<T>,
{
    fn push(&mut self, value: T) {
        self.out.push(value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        self.out.extend_from_slice(values)
    }
}

// ---

pub struct UnbufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    out: &'a mut O,
    padding: Padding<T>,
    cur: usize,
}

impl<'a, T, O> UnbufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    pub fn new(out: &'a mut O, padding: Padding<T>) -> Self {
        Self {
            out,
            padding,
            cur: 0,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.cur < self.padding.width {
            self.out.push(value);
            self.cur += 1;
        }
    }

    pub fn extend_from_slice(&mut self, values: &[T]) {
        if self.cur < self.padding.width {
            let n = min(self.padding.width - self.cur, values.len());
            self.out.extend_from_slice(&values[..n]);
            self.cur += n;
        }
    }
}

impl<'a, T, O> Push<T> for UnbufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn push(&mut self, value: T) {
        UnbufferedAligner::push(self, value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        UnbufferedAligner::extend_from_slice(self, values)
    }
}

impl<'a, T, O> Drop for UnbufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn drop(&mut self) {
        for _ in self.cur..self.padding.width {
            self.out.push(self.padding.pad.clone());
        }
    }
}

// ---

pub struct BufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    out: &'a mut O,
    padding: Padding<T>,
    alignment: Alignment,
    buf: AlignerBuffer<T>,
}

impl<'a, T, O> BufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn new(out: &'a mut O, padding: Padding<T>, alignment: Alignment) -> Self {
        Self {
            out,
            padding: padding.clone(),
            alignment,
            buf: if padding.width <= 64 {
                AlignerBuffer::Static(heapless::Vec::new())
            } else {
                AlignerBuffer::Dynamic(Vec::with_capacity(padding.width))
            },
        }
    }

    pub fn push(&mut self, value: T) {
        match self.buf {
            AlignerBuffer::Static(ref mut buf) => {
                if buf.len() < self.padding.width {
                    buf.push(value).ok();
                }
            }
            AlignerBuffer::Dynamic(ref mut buf) => {
                if buf.len() < self.padding.width {
                    buf.push(value);
                }
            }
        }
    }

    pub fn extend_from_slice(&mut self, values: &[T]) {
        match self.buf {
            AlignerBuffer::Static(ref mut buf) => {
                let n = min(self.padding.width - buf.len(), values.len());
                buf.extend_from_slice(&values[..n]).ok();
            }
            AlignerBuffer::Dynamic(ref mut buf) => {
                let n = min(self.padding.width - buf.len(), values.len());
                buf.extend_from_slice(&values[..n]);
            }
        }
    }
}

impl<'a, T, O> Push<T> for BufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn push(&mut self, value: T) {
        BufferedAligner::push(self, value)
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        BufferedAligner::extend_from_slice(self, values)
    }
}

impl<'a, T, O> Drop for BufferedAligner<'a, T, O>
where
    T: Clone,
    O: Push<T>,
{
    fn drop(&mut self) {
        let buf = match &self.buf {
            AlignerBuffer::Static(buf) => &buf[..],
            AlignerBuffer::Dynamic(buf) => &buf[..],
        };
        let offset = match self.alignment {
            Alignment::Left => 0,
            Alignment::Center => (self.padding.width - buf.len()) / 2,
            Alignment::Right => self.padding.width - buf.len(),
        };
        for _ in 0..offset {
            self.out.push(self.padding.pad.clone());
        }
        self.out.extend_from_slice(buf);
        for _ in offset + buf.len()..self.padding.width {
            self.out.push(self.padding.pad.clone());
        }
    }
}

// ---

enum AlignerBuffer<T> {
    Static(heapless::Vec<T, 64>),
    Dynamic(Vec<T>),
}

// ---

pub fn aligned<'a, T, O, F>(out: &'a mut O, adjustment: Option<Adjustment<T>>, f: F)
where
    T: Clone,
    O: Push<T>,
    F: FnOnce(Aligner<'a, T, O>),
{
    f(Aligner::new(out, adjustment));
}

pub fn aligned_left<'a, T, O, F>(out: &'a mut O, width: usize, pad: T, f: F)
where
    T: Clone,
    O: Push<T>,
    F: FnOnce(UnbufferedAligner<'a, T, O>),
{
    f(UnbufferedAligner::new(out, Padding::new(pad, width)));
}

pub fn centered<'a, T, O, F>(out: &'a mut O, width: usize, pad: T, f: F)
where
    T: Clone,
    O: Push<T>,
    F: FnOnce(BufferedAligner<'a, T, O>),
{
    f(BufferedAligner::new(
        out,
        Padding::new(pad, width),
        Alignment::Center,
    ));
}
