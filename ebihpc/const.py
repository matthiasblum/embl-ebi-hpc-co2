PUE = 1.2                  # https://kaodata.com/sustainability
# Values from green-algorithms.org
CPU_POWER = 6.3            # Intel Gold 6252 (6.3W/core)
GPU_POWER = 300            # NVIDIA Tesla V100 (300W/core)
MEM_POWER = 0.3725         # (W/GB)
CARBON_INTENSITY = 231.12  # UK (gCO2e/kWh)
ENERGY_COST = 0.34         # per kWh


def calc_footprint(energy_kw: float, runtime_h: float) -> tuple[float, float]:
    energy_needed = runtime_h * energy_kw * PUE
    carbon_footprint = energy_needed * CARBON_INTENSITY
    energy_cost = energy_needed * ENERGY_COST
    return carbon_footprint, energy_cost
