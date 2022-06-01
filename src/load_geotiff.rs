use geotiff::TIFF;

/*
pub fn pixel_index(x1: usize, y1: usize, gtiff: TIFF) -> usize {
    42
}
*/

pub fn load_pixel(x1: usize, y1: usize, filepath: &str) -> usize {
    let gtiff = TIFF::open(filepath);
    println!("done opening file");
    match gtiff {
        Ok(g) => g.get_value_at(x1, y1),
        Err(_error) => panic!("Couldn't open {}", filepath),
    }
}

#[cfg(test)]
pub mod tests {
    use super::load_pixel;

    /*
    #[test]
    pub fn test_load_geotiff() {
        let px = load_pixel(1, 1, "/RAID0/ais/gebco_2021_n0.0_s-90.0_w0.0_e90.0.tif");
        println!("{}", px);
    }
    */
}
