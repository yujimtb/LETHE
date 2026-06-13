use std::io::Cursor;

use image::ImageFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CropRegion {
    pub left: u32,
    pub top: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CropType {
    Profile,
    Gallery,
}

#[derive(Debug, thiserror::Error)]
pub enum CropError {
    #[error("invalid crop coordinates ({x}, {y})")]
    InvalidCoordinates { x: f64, y: f64 },
    #[error("image decode error: {0}")]
    Decode(#[from] image::ImageError),
    #[error("image encode error: {0}")]
    Encode(#[from] std::io::Error),
}

pub fn crop_from_thumbnail(
    thumbnail_png: &[u8],
    center_x_pct: f64,
    center_y_pct: f64,
    crop_type: CropType,
) -> Result<Vec<u8>, CropError> {
    let image = image::load_from_memory(thumbnail_png)?;
    let rgba = image.to_rgba8();
    let region = crop_region(rgba.width(), rgba.height(), center_x_pct, center_y_pct, crop_type)?;
    let cropped = image::imageops::crop_imm(&rgba, region.left, region.top, region.width, region.height).to_image();
    let mut bytes = Vec::new();
    image::DynamicImage::ImageRgba8(cropped).write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)?;
    Ok(bytes)
}

fn crop_region(
    width: u32,
    height: u32,
    center_x_pct: f64,
    center_y_pct: f64,
    crop_type: CropType,
) -> Result<CropRegion, CropError> {
    let center_x_pct = normalize_coordinate_pct(center_x_pct)?;
    let center_y_pct = normalize_coordinate_pct(center_y_pct)?;
    if !(0.0..=100.0).contains(&center_x_pct) || !(0.0..=100.0).contains(&center_y_pct) {
        return Err(CropError::InvalidCoordinates {
            x: center_x_pct,
            y: center_y_pct,
        });
    }

    let min_edge = width.min(height) as f64;
    let size_ratio = match crop_type {
        CropType::Profile => 0.25,
        CropType::Gallery => 0.30,
    };
    let mut crop_size = (min_edge * size_ratio).round().max(1.0) as u32;
    crop_size = crop_size.min(width).min(height).max(1);

    let center_x = ((center_x_pct / 100.0) * width as f64).round() as i64;
    let center_y = ((center_y_pct / 100.0) * height as f64).round() as i64;
    let half = (crop_size / 2) as i64;

    let max_left = width.saturating_sub(crop_size) as i64;
    let max_top = height.saturating_sub(crop_size) as i64;
    let left = (center_x - half).clamp(0, max_left) as u32;
    let top = (center_y - half).clamp(0, max_top) as u32;

    Ok(CropRegion {
        left,
        top,
        width: crop_size,
        height: crop_size,
    })
}

fn normalize_coordinate_pct(value: f64) -> Result<f64, CropError> {
    if (0.0..=100.0).contains(&value) {
        Ok(value)
    } else if (0.0..=1000.0).contains(&value) {
        Ok(value / 10.0)
    } else {
        Err(CropError::InvalidCoordinates { x: value, y: value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_png(width: u32, height: u32) -> Vec<u8> {
        let image = image::RgbaImage::from_fn(width, height, |x, y| image::Rgba([
            (x % 255) as u8,
            (y % 255) as u8,
            120,
            255,
        ]));
        let mut bytes = Vec::new();
        image::DynamicImage::ImageRgba8(image)
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .unwrap();
        bytes
    }

    #[test]
    fn crop_center_returns_square() {
        let region = crop_region(1920, 1080, 50.0, 50.0, CropType::Gallery).unwrap();
        assert_eq!(region.width, region.height);
        assert!(region.width > 0);
    }

    #[test]
    fn crop_edge_clamps_to_bounds() {
        let bytes = sample_png(400, 300);
        let cropped = crop_from_thumbnail(&bytes, 0.0, 0.0, CropType::Profile).unwrap();
        let decoded = image::load_from_memory(&cropped).unwrap();
        assert!(decoded.width() > 0);
        assert!(decoded.height() > 0);
    }

    #[test]
    fn profile_crop_smaller_than_gallery() {
        let profile = crop_region(1000, 800, 50.0, 50.0, CropType::Profile).unwrap();
        let gallery = crop_region(1000, 800, 50.0, 50.0, CropType::Gallery).unwrap();
        assert!(profile.width < gallery.width);
    }

    #[test]
    fn legacy_thousand_scale_coordinates_are_supported() {
        let region = crop_region(1000, 800, 750.0, 250.0, CropType::Profile).unwrap();
        assert!(region.width > 0);
        assert!(region.height > 0);
    }
}
