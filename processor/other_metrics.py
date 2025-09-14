# def _helper(column: str, params: ExecutionParams) -> StructResult:
#     trip_id = params.window.metadata["trip_id"]
#     if trip_id is None:
#         raise Exception("Require trip_id as metadata to the window")
#
#     telem = ReadTelemetryForTripAndTime(
#         ReadTelemParams(
#             time_from=params.window.time_from,
#             time_to=params.window.time_to,
#             trip_id=trip_id,
#         )
#     )
#     df = pd.DataFrame(telem)
#     if df.empty:
#         return StructResult(
#             {
#                 "mean": None,
#                 "std": None,
#                 "min": None,
#                 "25p": None,
#                 "50p": None,
#                 "75p": None,
#                 "max": None,
#             }
#         )
#
#     stats = df[column].describe()
#     return StructResult(
#         {
#             "mean": stats["mean"],
#             "std": stats["std"],
#             "min": stats["min"],
#             "25p": stats["25%"],
#             "50p": stats["50%"],
#             "75p": stats["75%"],
#             "max": stats["max"],
#         }
#     )
#
#
# @proc.algorithm("ElectricPowerDemandHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def electric_power_demand_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("electric_power_demand", params)
#
#
# @proc.algorithm("TractionBrakePressureHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def traction_brake_pressure_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("traction_brake_pressure", params)
#
#
# @proc.algorithm("TractionTractionForceHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def traction_traction_force_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("traction_traction_force", params)
#
#
# @proc.algorithm("GnssAltitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def gnss_altitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_altitude", params)
#
#
# @proc.algorithm("GnssCourseHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def gnss_course_halt_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_course", params)
#
#
# @proc.algorithm("GnssLatitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def gnss_latitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_latitude", params)
#
#
# @proc.algorithm("GnssLongitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def gnss_longitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_longitude", params)
#
#
# @proc.algorithm("OdometryArticulationAngleHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_articulation_angle_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_articulation_angle", params)
#
#
# @proc.algorithm("OdometrySteeringAngleHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_steering_angle_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_steering_angle", params)
#
#
# @proc.algorithm("OdometryVehicleSpeedHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_vehicle_speed_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_vehicle_speed", params)
#
#
# @proc.algorithm("OdometryWheelSpeedFlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_fl_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_fl", params)
#
#
# @proc.algorithm("OdometryWheelSpeedFrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_fr_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_fr", params)
#
#
# @proc.algorithm("OdometryWheelSpeedMlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_ml_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_ml", params)
#
#
# @proc.algorithm("OdometryWheelSpeedMrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_mr_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_mr", params)
#
#
# @proc.algorithm("OdometryWheelSpeedRlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_rl_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_rl", params)
#
#
# @proc.algorithm("OdometryWheelSpeedRrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
# def odometry_wheel_speed_rr_halt_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_rr", params)
#
#
# @proc.algorithm("ElectricPowerDemandParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def electric_power_demand_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("electric_power_demand", params)
#
#
# @proc.algorithm("TractionBrakePressureParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def traction_brake_pressure_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("traction_brake_pressure", params)
#
#
# @proc.algorithm("TractionTractionForceParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def traction_traction_force_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("traction_traction_force", params)
#
#
# @proc.algorithm("GnssAltitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def gnss_altitude_park_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_altitude", params)
#
#
# @proc.algorithm("GnssCourseParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def gnss_course_park_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_course", params)
#
#
# @proc.algorithm("GnssLatitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def gnss_latitude_park_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_latitude", params)
#
#
# @proc.algorithm("GnssLongitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def gnss_longitude_park_brake_stats(params: ExecutionParams) -> StructResult:
#     return _helper("gnss_longitude", params)
#
#
# @proc.algorithm("OdometryArticulationAngleParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_articulation_angle_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_articulation_angle", params)
#
#
# @proc.algorithm("OdometrySteeringAngleParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_steering_angle_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_steering_angle", params)
#
#
# @proc.algorithm("OdometryVehicleSpeedParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_vehicle_speed_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_vehicle_speed", params)
#
#
# @proc.algorithm("OdometryWheelSpeedFlParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_fl_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_fl", params)
#
#
# @proc.algorithm("OdometryWheelSpeedFrParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_fr_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_fr", params)
#
#
# @proc.algorithm("OdometryWheelSpeedMlParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_ml_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_ml", params)
#
#
# @proc.algorithm("OdometryWheelSpeedMrParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_mr_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_mr", params)
#
#
# @proc.algorithm("OdometryWheelSpeedRlParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_rl_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_rl", params)
#
#
# @proc.algorithm("OdometryWheelSpeedRrParkBrakeStats", "1.0.0", ParkBrakeApplied)
# def odometry_wheel_speed_rr_park_brake_stats(
#     params: ExecutionParams,
# ) -> StructResult:
#     return _helper("odometry_wheel_speed_rr", params)
