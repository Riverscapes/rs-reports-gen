def hex_to_rgb(hex_color):
    """Convert hex to RGB"""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def gradient_color(start_hex, end_hex, t):
    """
    Returns a hex color interpolated between start_hex and end_hex.
    t: float between 0 (start) and 1 (end)

    # Example usage:
    color = gradient_color('#90ee90', '#00008b', 0.5)  # Midpoint color
    """

    # Convert RGB to hex
    def rgb_to_hex(rgb):
        return '#{:02x}{:02x}{:02x}'.format(*rgb)

    start_rgb = hex_to_rgb(start_hex)
    end_rgb = hex_to_rgb(end_hex)
    interp_rgb = tuple(
        int(start + (end - start) * t)
        for start, end in zip(start_rgb, end_rgb)
    )
    return rgb_to_hex(interp_rgb)


def value_as_prop(val: float, low_bound: float, up_bound: float):
    """return where value is along a gradient from low bound to up bound, from 0 to 1"""
    if up_bound <= low_bound:
        raise ValueError(f"Upper bound ({up_bound}) must be strictly greater than lower bound ({low_bound}).")
    if val < low_bound or val > up_bound:
        raise ValueError(f"Value {val} was expected to be between {low_bound} and {up_bound} (inclusive).")
    fraction = (val - low_bound) / (up_bound-low_bound)
    return fraction


def stream_colour_from_order(stream_order: float):
    """return a colour based on a green-to-blue gradient from lowest stream order (1) to highest stream order (11)"""
    min_order = 1.0
    max_order = 11.0

    stream_colour = gradient_color('#90ee90', '#00008b', value_as_prop(stream_order, min_order, max_order))
    return stream_colour


def preview_stream_order_colours(min_order=1, max_order=11):
    """Prints stream_order and corresponding colour for quick checking."""
    for order in range(min_order, max_order + 1):
        try:
            colour = stream_colour_from_order(order)
            rgb = hex_to_rgb(colour)
            ansi = f"\033[38;2;{rgb[0]};{rgb[1]};{rgb[2]}m"
            reset = "\033[0m"
            print(f"stream_order: {order} -> {colour} {ansi}██████{reset}")
        except Exception as e:
            print(e)
            continue


if __name__ == '__main__':
    preview_stream_order_colours()
