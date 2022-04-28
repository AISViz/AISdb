use pyo3::prelude::*;
use geo::prelude::*;
use geo::{point};


#[pyfunction]
pub fn _haversine(x1:f64, y1:f64, x2:f64, y2:f64) -> f64{
    let p1 = point!(x:x1, y:y1);
    let p2 = point!(x:x2, y:y2);
    p1.haversine_distance(&p2)
}

#[pymodule] #[allow(unused_variables)]
pub fn aisdb_extra(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(_haversine))?;
    Ok(())
}


// pip/pac install maturin
// maturin develop
