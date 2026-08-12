//! One compile root for the suites, in the reference layout: topic
//! files under `cases/`. Only `crash_sweep` keeps its own binary — the
//! by-hand sweep selects it by name (023's recorded convention); no
//! gate selects anything here by binary, so consolidation costs nothing
//! and saves link steps.

mod cases;
