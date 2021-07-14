// std imports
use std::{collections::HashMap, io::Write};

// third-party imports
use bitflags::bitflags;
use bitmask::bitmask;

// local imports
use crate::fmtx::Push;

// ---

pub trait PushAnnotatedData {
    fn push_annotated(&mut self, data: u8, annotations: Annotations);
    fn extend_from_slice_annotated(&mut self, data: &[u8], annotations: Annotations);
}

pub trait ProcessSGR: Push<u8> + PushAnnotatedData {
    fn push_instruction(&mut self, instruction: Instruction);
}

// ---

bitflags! {
    pub struct Annotations: u8 {
        const UsesForeground = 1 << 0;
        const UsesBackground = 1 << 1;
    }
}

// ---

bitmask! {
    #[derive(Debug,Default)]
    pub mask Flags: u16 where flags Flag {
        Bold                = 1 << 0,
        Faint               = 1 << 1,
        Italic              = 1 << 2,
        Underlined          = 1 << 3,
        SlowBlink           = 1 << 4,
        RapidBlink          = 1 << 5,
        Reversed            = 1 << 6,
        Concealed           = 1 << 7,
        CrossedOut          = 1 << 8,
        DoublyUnderlined    = 1 << 9,
        Framed              = 1 << 10,
        Encircled           = 1 << 11,
        Overlined           = 1 << 12,
        Superscript         = 1 << 13,
        Subscript           = 1 << 14,
    }
}

// ---

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum CommandCode {
    // ResetAll = 0,
    SetBold = 1,
    SetFaint = 2,
    SetItalic = 3,
    SetUnderlined = 4,
    SetSlowBlink = 5,
    SetRapidBlink = 6,
    SetReversed = 7,
    SetConcealed = 8,
    SetCrossedOut = 9,
    SetDoublyUnderlined = 21,
    ResetBoldAndFaint = 22,
    ResetItalic = 23,
    ResetAllUnderlines = 24,
    ResetAllBlinks = 25,
    ResetReversed = 27,
    ResetConcealed = 28,
    ResetCrossedOut = 29,
    SetFirstForegroundColor = 30,
    ResetForegroundColor = 39,
    SetFirstBackgroundColor = 40,
    ResetBackgroundColor = 49,
    SetFramed = 51,
    SetEncircled = 52,
    SetOverlined = 53,
    ResetFramedAndEncircled = 54,
    ResetOverlined = 55,
    SetUnderlineColor = 58,
    ResetUnderlineColor = 59,
    SetSuperscript = 73,
    SetSubscript = 74,
    ResetSuperscriptAndSubscript = 75,
    SetFirstBrightForegroundColor = 90,
    SetFirstBrightBackgroundColor = 100,
}

impl CommandCode {
    fn render(&self, buf: &mut Vec<u8>) {
        write!(buf, "{}", (*self as u8)).unwrap()
    }
}

// ---

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum BasicColor {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
}

impl BasicColor {
    #[inline(always)]
    pub fn bright(self) -> PlainColor {
        PlainColor(self, Brightness::Bright)
    }

    #[inline(always)]
    pub fn fg(self) -> (Instruction, Instruction) {
        Color::Plain(self, Brightness::Normal).fg()
    }

    #[inline(always)]
    pub fn bg(self) -> (Instruction, Instruction) {
        Color::Plain(self, Brightness::Normal).bg()
    }

    fn render(&self, buf: &mut Vec<u8>, base: u8) {
        write!(buf, "{}", base + (*self as u8)).unwrap()
    }
}

// ---

pub struct PlainColor(BasicColor, Brightness);

impl PlainColor {
    #[inline(always)]
    pub fn fg(self) -> (Instruction, Instruction) {
        Color::Plain(self.0, self.1).fg()
    }

    #[inline(always)]
    pub fn bg(self) -> (Instruction, Instruction) {
        Color::Plain(self.0, self.1).bg()
    }
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Color {
    Default,
    Plain(BasicColor, Brightness),
    Palette(u8),
    RGB(u8, u8, u8),
}

impl Color {
    #[inline(always)]
    pub fn fg(self) -> (Instruction, Instruction) {
        (
            Instruction::PushForeground(self),
            Instruction::PopForeground,
        )
    }

    #[inline(always)]
    pub fn bg(self) -> (Instruction, Instruction) {
        (
            Instruction::PushBackground(self),
            Instruction::PopBackground,
        )
    }

    fn render(&self, buf: &mut Vec<u8>, base: u8) {
        match self {
            Self::Default => write!(buf, "{}", base + 9).unwrap(),
            Self::Plain(color, Brightness::Normal) => color.render(buf, base),
            Self::Plain(color, Brightness::Bright) => color.render(buf, base + 60),
            Self::Palette(color) => write!(buf, "{};5;{}", base + 8, color).unwrap(),
            Self::RGB(r, g, b) => write!(buf, "{};2;{};{};{}", base + 8, r, g, b).unwrap(),
        }
    }
}

impl Default for Color {
    fn default() -> Self {
        Self::Default
    }
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Brightness {
    Normal,
    Bright,
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Command {
    Plain(CommandCode),
    SetBackground(Color),
    SetForeground(Color),
}

impl Command {
    #[inline(always)]
    fn render(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Plain(code) => code.render(buf),
            Self::SetBackground(command) => command.render(buf, 40),
            Self::SetForeground(command) => command.render(buf, 30),
        }
    }
}

impl From<CommandCode> for Command {
    #[inline(always)]
    fn from(code: CommandCode) -> Self {
        Self::Plain(code)
    }
}

impl Into<Vec<u8>> for Command {
    #[inline(always)]
    fn into(self) -> Vec<u8> {
        let mut result = Vec::new();
        self.render(&mut result);
        result
    }
}

// ---

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Instruction {
    ResetAll,
    PushFlags(Flags, Operator),
    PopFlags,
    PushBackground(Color),
    PopBackground,
    PushForeground(Color),
    PopForeground,
}

// ---

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Style {
    pub flags: Option<(Flags, Operator)>,
    pub background: Option<Color>,
    pub foreground: Option<Color>,
}

// ---

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operator {
    Set,
    And,
    Or,
    Xor,
}

// ---

#[derive(Clone, Eq, PartialEq)]
pub struct Sequence {
    buf: Vec<u8>,
}

impl Sequence {
    pub fn reset() -> Self {
        let mut buf = Vec::with_capacity(5);
        begin(&mut buf);
        end(&mut buf);
        Self { buf }
    }

    pub fn data(&self) -> &[u8] {
        &self.buf
    }
}

impl From<Vec<u8>> for Sequence {
    fn from(buf: Vec<u8>) -> Self {
        Self { buf }
    }
}

impl From<Vec<Command>> for Sequence {
    fn from(commands: Vec<Command>) -> Self {
        let mut buf = Vec::new();
        begin(&mut buf);
        for command in commands {
            next(&mut buf);
            command.render(&mut buf);
        }
        end(&mut buf);
        Self { buf }
    }
}

// ---

#[derive(Default)]
pub struct Cache(HashMap<Command, Vec<u8>>);

// ---

pub struct Processor<'c, O: Push<u8> + 'c, const N: usize> {
    cache: &'c mut Cache,
    output: O,
    flags: State<Flags, N>,
    bg: State<Color, N>,
    fg: State<Color, N>,
    dirty: bool,
}

impl<'c, O: Push<u8> + 'c, const N: usize> Processor<'c, O, N> {
    pub fn new(cache: &'c mut Cache, output: O) -> Self {
        Self {
            cache,
            output,
            flags: State::default(),
            bg: State::default(),
            fg: State::default(),
            dirty: false,
        }
    }

    #[inline(always)]
    fn soil(&mut self) -> &mut Self {
        self.dirty = true;
        self
    }

    fn sync(&mut self, annotations: Annotations) {
        if !self.dirty {
            return;
        }

        let mut csb = CommandSequenceBuilder::new(&mut self.output, self.cache);
        let bg = self.bg.stack.last().copied().unwrap_or_default();
        let fg = self.fg.stack.last().copied().unwrap_or_default();
        let flags = self.flags.stack.last().copied().unwrap_or_default();
        // println!("bg={:?} synced={:?}", bg, self.bg.synced);
        if self.bg.synced != bg && annotations.contains(Annotations::UsesBackground) {
            csb.append(Command::SetBackground(bg));
            self.bg.synced = bg;
        }
        // println!("fg={:?} synced={:?}", fg, self.fg.synced);
        if self.fg.synced != fg && annotations.contains(Annotations::UsesForeground) {
            csb.append(Command::SetForeground(fg));
            self.fg.synced = fg;
        }
        if self.flags.synced != flags {
            self.dirty = false;
            let mut diff = self.flags.synced ^ flags;
            for (f0, f1, set0, set1, reset, a) in DUAL_SYNC_TABLE {
                if !a.intersects(annotations) {
                    diff.unset(*f0 | *f1);
                    self.dirty = true;
                    continue;
                }
                let actions = dual_flag_sync(diff, flags, *f0, *f1);
                if actions.2 {
                    csb.append((*reset).into());
                }
                if actions.0 {
                    csb.append((*set0).into());
                }
                if actions.1 {
                    csb.append((*set1).into());
                }
            }
            for (f, set, reset, a) in SINGLE_SYNC_TABLE {
                if !a.intersects(annotations) {
                    diff.unset(*f);
                    self.dirty = true;
                    continue;
                }
                if diff.contains(*f) {
                    csb.append(if flags.contains(*f) { *set } else { *reset }.into());
                }
            }
            self.flags.synced.unset(diff);
            self.flags.synced.set(flags & diff);
        }
    }
}

impl<'c, O: Push<u8> + 'c, const N: usize> Push<u8> for Processor<'c, O, N> {
    #[inline]
    fn push(&mut self, data: u8) {
        self.sync(Annotations::all());
        self.output.push(data);
    }
    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) {
        self.sync(Annotations::all());
        self.output.extend_from_slice(data);
    }
}

impl<'c, O: Push<u8> + 'c, const N: usize> PushAnnotatedData for Processor<'c, O, N> {
    #[inline]
    fn push_annotated(&mut self, data: u8, annotations: Annotations) {
        self.sync(annotations);
        self.output.push(data);
    }
    #[inline]
    fn extend_from_slice_annotated(&mut self, data: &[u8], annotations: Annotations) {
        self.sync(annotations);
        self.output.extend_from_slice(data);
    }
}

impl<'c, O: Push<u8> + 'c, const N: usize> Drop for Processor<'c, O, N> {
    #[inline]
    fn drop(&mut self) {
        self.output.extend_from_slice(RESET);
    }
}

impl<'c, O: Push<u8> + 'c, const N: usize> ProcessSGR for Processor<'c, O, N> {
    #[inline]
    fn push_instruction(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::ResetAll => {
                self.flags = State::default();
                self.bg = State::default();
                self.fg = State::default();
                self.output.extend_from_slice(RESET);
            }
            Instruction::PushFlags(flags, operator) => {
                let mut f = self.flags.stack.last().cloned().unwrap_or_default();
                match operator {
                    Operator::Set => f = flags,
                    Operator::And => f &= flags,
                    Operator::Or => f |= flags,
                    Operator::Xor => f ^= flags,
                };
                self.soil().flags.stack.push(f).unwrap();
            }
            Instruction::PopFlags => {
                self.soil().flags.stack.pop().unwrap();
            }
            Instruction::PushBackground(color) => {
                self.soil().bg.stack.push(color).unwrap();
            }
            Instruction::PopBackground => {
                self.soil().bg.stack.pop().unwrap();
            }
            Instruction::PushForeground(color) => {
                // println!("PushForeground {:?}", color);
                self.soil().fg.stack.push(color).unwrap();
            }
            Instruction::PopForeground => {
                // println!("PopForeground");
                self.soil().fg.stack.pop().unwrap();
            }
        }
    }
}

// ---

struct CommandSequenceBuilder<'a, O: Push<u8> + 'a> {
    output: &'a mut O,
    cache: &'a mut Cache,
    first: bool,
}

impl<'a, O: Push<u8> + 'a> CommandSequenceBuilder<'a, O> {
    #[inline]
    fn new(output: &'a mut O, cache: &'a mut Cache) -> Self {
        Self {
            output,
            cache,
            first: true,
        }
    }

    #[inline]
    fn append(&mut self, command: Command) {
        // println!("BEGIN or NEXT: {:?}", command);
        self.output
            .extend_from_slice(if self.first { BEGIN } else { NEXT });
        self.first = false;
        let data = self
            .cache
            .0
            .entry(command)
            .or_insert_with(|| command.into());
        self.output.extend_from_slice(data);
    }
}

impl<'a, O: Push<u8> + 'a> Drop for CommandSequenceBuilder<'a, O> {
    #[inline]
    fn drop(&mut self) {
        if !self.first {
            // println!("END");
            self.output.extend_from_slice(END);
        }
    }
}

// ---

#[derive(Default)]
struct State<T: Copy, const N: usize> {
    synced: T,
    stack: heapless::Vec<T, N>,
}

// ---

const BEGIN: &[u8] = b"\x1b[";
const NEXT: &[u8] = b";";
const END: &[u8] = b"m";
const RESET: &[u8] = b"\x1b[m";

const SINGLE_SYNC_TABLE: &[(Flag, CommandCode, CommandCode, Annotations)] = &[
    (
        Flag::Italic,
        CommandCode::SetItalic,
        CommandCode::ResetItalic,
        Annotations::UsesForeground,
    ),
    (
        Flag::Concealed,
        CommandCode::SetConcealed,
        CommandCode::ResetConcealed,
        Annotations::UsesForeground,
    ),
    (
        Flag::CrossedOut,
        CommandCode::SetCrossedOut,
        CommandCode::ResetCrossedOut,
        Annotations::UsesForeground,
    ),
    (
        Flag::Reversed,
        CommandCode::SetReversed,
        CommandCode::ResetReversed,
        Annotations::all(),
    ),
    (
        Flag::Overlined,
        CommandCode::SetOverlined,
        CommandCode::ResetOverlined,
        Annotations::UsesForeground,
    ),
];

const DUAL_SYNC_TABLE: &[(
    Flag,
    Flag,
    CommandCode,
    CommandCode,
    CommandCode,
    Annotations,
)] = &[
    (
        Flag::Bold,
        Flag::Faint,
        CommandCode::SetBold,
        CommandCode::SetFaint,
        CommandCode::ResetBoldAndFaint,
        Annotations::UsesForeground,
    ),
    (
        Flag::Underlined,
        Flag::DoublyUnderlined,
        CommandCode::SetUnderlined,
        CommandCode::SetDoublyUnderlined,
        CommandCode::ResetAllUnderlines,
        Annotations::UsesForeground,
    ),
    (
        Flag::SlowBlink,
        Flag::RapidBlink,
        CommandCode::SetSlowBlink,
        CommandCode::SetRapidBlink,
        CommandCode::ResetAllBlinks,
        Annotations::all(),
    ),
    (
        Flag::Framed,
        Flag::Encircled,
        CommandCode::SetFramed,
        CommandCode::SetEncircled,
        CommandCode::ResetFramedAndEncircled,
        Annotations::all(),
    ),
    (
        Flag::Subscript,
        Flag::Superscript,
        CommandCode::SetSubscript,
        CommandCode::SetSuperscript,
        CommandCode::ResetSuperscriptAndSubscript,
        Annotations::UsesForeground,
    ),
];

// ---

#[inline]
fn dual_flag_sync(mut diff: Flags, flags: Flags, f0: Flag, f1: Flag) -> (bool, bool, bool) {
    let mut result = (false, false, false);
    if !diff.intersects(f0 | f1) {
        return result;
    }
    if ((flags ^ diff) & diff).intersects(f0 | f1) {
        result.2 = true;
        diff |= flags & (f0 | f1);
    }
    if (diff & flags).contains(f0) {
        result.0 = true;
    }
    if (diff & flags).contains(f1) {
        result.1 = true;
    }
    result
}

#[inline]
fn begin(buf: &mut Vec<u8>) {
    buf.extend_from_slice(BEGIN);
}

#[inline]
fn next(buf: &mut Vec<u8>) {
    buf.extend_from_slice(NEXT);
}

#[inline]
fn end(buf: &mut Vec<u8>) {
    buf.extend_from_slice(END);
}

// ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dual_flag_sync() {
        //        CUR  NEW  CMD     CODE
        //   [0]  0/0  0/0           0
        //   [1]  0/0  0/1  S1       1
        //   [2]  0/0  1/0  S2       2
        //   [3]  0/0  1/1  S1,S2    1|2
        //   [4]  0/1  0/0  R        4
        //   [5]  0/1  0/1           0
        //   [6]  0/1  1/0  R,S2     4|2
        //   [7]  0/1  1/1  S2       2
        //   [8]  1/0  0/0  R        4
        //   [9]  1/0  0/1  R,S1     4|1
        //  [10]  1/0  1/0           0
        //  [11]  1/0  1/1  S1       1
        //  [12]  1/1  0/0  R        4
        //  [13]  1/1  0/1  R,S1     4|1
        //  [14]  1/1  1/0  R,S2     4|2
        //  [15]  1/1  1/1           0
        let table = [
            (Flags::none(), Flags::none(), (false, false, false)),
            (Flags::none(), Flag::Bold.into(), (true, false, false)),
            (Flags::none(), Flag::Faint.into(), (false, true, false)),
            (
                Flags::none(),
                (Flag::Bold | Flag::Faint).into(),
                (true, true, false),
            ),
            (Flag::Bold.into(), Flags::none(), (false, false, true)),
            (Flag::Bold.into(), Flag::Bold.into(), (false, false, false)),
            (Flag::Bold.into(), Flag::Faint.into(), (false, true, true)),
            (
                Flag::Bold.into(),
                (Flag::Bold | Flag::Faint).into(),
                (false, true, false),
            ),
            (Flag::Faint.into(), Flags::none(), (false, false, true)),
            (Flag::Faint.into(), Flag::Bold.into(), (true, false, true)),
            (
                Flag::Faint.into(),
                Flag::Faint.into(),
                (false, false, false),
            ),
            (
                Flag::Faint.into(),
                (Flag::Bold | Flag::Faint).into(),
                (true, false, false),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flags::none(),
                (false, false, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flag::Bold.into(),
                (true, false, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flag::Faint.into(),
                (false, true, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                (Flag::Bold | Flag::Faint).into(),
                (false, false, false),
            ),
        ];
        for (current, flags, expected) in table {
            let result = dual_flag_sync(current ^ flags, flags, Flag::Bold, Flag::Faint);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_processor() {
        let mut output = Vec::<u8>::new();
        let mut cache = Cache::default();
        let mut processor = Processor::<_, 16>::new(&mut cache, &mut output);
        processor.push_instruction(Instruction::PushForeground(Color::Plain(
            BasicColor::Green,
            Brightness::Normal,
        )));
        processor.extend_from_slice(b"hello");
        processor.push(b',');
        processor.push(b' ');
        processor.push_instruction(Instruction::PushForeground(Color::Plain(
            BasicColor::Green,
            Brightness::Normal,
        )));
        processor.extend_from_slice(b"world");
        processor.push_instruction(Instruction::PopForeground);
        processor.push_instruction(Instruction::PopForeground);
        drop(processor);
        assert_eq!(output, b"\x1b[32mhello, world\x1b[m")
    }
}
