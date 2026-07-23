/// Format a USD cost value for display.
///
/// Small non-zero values are shown as `<$0.001`, values under a dollar use
/// three decimal places, and larger values use two.
pub(crate) fn format_cost(cost: f64) -> String {
    if cost > 0.0 && cost < 0.001 {
        "<$0.001".to_string()
    } else if cost < 1.0 {
        format!("${cost:.3}")
    } else {
        format!("${cost:.2}")
    }
}

#[cfg(test)]
mod tests {
    use super::format_cost;

    #[test]
    fn formats_sub_millicent_costs() {
        assert_eq!(format_cost(0.0004), "<$0.001");
    }

    #[test]
    fn formats_sub_dollar_costs_with_three_decimals() {
        assert_eq!(format_cost(0.123), "$0.123");
    }

    #[test]
    fn formats_larger_costs_with_two_decimals() {
        assert_eq!(format_cost(12.3456), "$12.35");
    }

    #[test]
    fn formats_zero_as_three_decimals() {
        assert_eq!(format_cost(0.0), "$0.000");
    }
}
