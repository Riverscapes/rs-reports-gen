import pint
from rsxml import Logger


# Initialize a UnitRegistry
# DO THIS ONLY ONCE
UREG = pint.UnitRegistry()
Q_ = UREG.Quantity


def pint_example():
    """
    Example function demonstrating basic usage of Pint for unit handling.
    https://pint.readthedocs.io/
    """
    logger = Logger("PintExample")
    logger.title("Pint Unit Conversion example")

    # Multiple ways to define a pint Quantity
    length = Q_(5, UREG.meter)  # using constructor
    time = 10 * UREG.second  # by multiplying a scalar by a Unit
    speed = length / time
    logger.info(f"Length: {length.to(UREG.centimeter)}")
    # [INFO] [PintExample] Length: 500.0 centimeter
    logger.info(f"Time: {time.to(UREG.minute)}")
    # [INFO] [PintExample] Time: 0.16666666666666666 minute
    logger.info(f"Speed: from {speed.to(UREG.meter / UREG.second)} to {speed.to(UREG.kilometer / UREG.hour)} to {speed.to(UREG.mile / UREG.hour)}")
    # [INFO] [PintExample] Speed: from 0.5 meter / second to 1.8 kilometer / hour to 1.1184681460272012 mile / hour

    # Volumes
    volume = 3 * UREG.liter
    logger.info(f"Volume: {volume.to(UREG.milliliter)}")
    # [INFO] [PintExample] Volume: 3000.0 milliliter

    volume2 = 3.234 * UREG.meters ** 3
    # Formatting: https://pint.readthedocs.io/en/stable/user/formatting.html
    logger.info(f"Volume: {volume2:2f}")
    # [INFO] [PintExample] Volume: 3.234000 meter ** 3
    logger.info(f"Volume: {volume2:~P}")  # Pretty print
    # [INFO] [PintExample] Volume: 3.234 m³
    logger.info(f"Volume: {volume2:~L}")  # LaTeX format
    # [INFO] [PintExample] Volume: 3.234\ \mathrm{m}^{3}
    logger.info(f"Volume: {volume2:~H}")  # HTML format
    # [INFO] [PintExample] Volume: 3.234 m<sup>3</sup>

    # Masses
    mass = 70 * UREG.kilogram
    logger.info(f"Mass: {mass.to(UREG.gram)}")
    # [INFO] [PintExample] Mass: 70000.0 gram

    # Areas
    area = 50 * UREG.kilometer ** 2
    logger.info(f"Area: {area}")
    # [INFO] [PintExample] Area: 50 kilometer ** 2
    logger.info(f"Area: {area.to(UREG.hectometer ** 2)}")
    # [INFO] [PintExample] Area: 5000.0 hectometer ** 2
    area = Q_(34, "km^2")
    logger.info(f"Area: {area} is {area.to("ha")} or {area.to("sq_mi"):~P}")

    # conversion, but return the magnitude only, then format it with commas and zero decimal places.
    logger.info(f"There are {Q_(1, "mile").to("feet").magnitude:,.0f} feet in a mile.")

    # Choose appropriate units
    byte_too_many = Q_(1234567890, UREG.byte)
    logger.info(f"Bytes: {byte_too_many.to_compact():.2f~#P}")  # short compact pretty with 2 float digits
    # [INFO] [PintExample] Bytes: 1.2 GB

    cm = Q_(12345678, UREG.centimeter)
    logger.info(f"Centimeters: {cm.to_compact():.2f~#P}")  # short compact pretty with 2 float digits
    # [INFO] [PintExample] Centimeters: 123.46 km

    # Unit Systems
    # List all available systems
    print("Available systems:", list(UREG._systems.keys()))

    print("Imperial units:")
    print(dir(UREG.sys.imperial))

    print("\nSI units:")
    print(dir(UREG.sys.SI))

    print("\nUS units:")
    print(dir(UREG.sys.US))


if __name__ == "__main__":
    pint_example()
