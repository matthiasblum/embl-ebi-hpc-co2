from datetime import datetime

PUE = 1.2                  # https://kaodata.com/sustainability
# Values from green-algorithms.org
CPU_POWER = 6.3            # Intel Gold 6252 (6.3W/core)
GPU_POWER = 300            # NVIDIA Tesla V100 (300W/core)
MEM_POWER = 0.3725         # (W/GB)
CARBON_INTENSITY = 231.12  # UK (gCO2e/kWh)
CARBON_INTENSITY_2023 = 207.074
ENERGY_COST = 0.34         # per kWh

MIN_MEM_REQ = 4096         # MB


def calc_footprint(energy_kw: float, runtime_h: float,
                   dt: datetime) -> tuple[float, float]:
    if dt >= datetime(2023, 1, 1):
        carb_int = CARBON_INTENSITY_2023
    else:
        carb_int = CARBON_INTENSITY

    energy_needed = runtime_h * energy_kw * PUE
    carbon_footprint = energy_needed * carb_int
    energy_cost = energy_needed * ENERGY_COST
    return carbon_footprint, energy_cost
