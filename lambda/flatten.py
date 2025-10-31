
import pandas as pd

def flatten_records(data):  
    rows = []
    for rec in data:
        airport_code = rec.get("Airport", {}).get("Code")
        airport_name = rec.get("Airport", {}).get("Name")
        time_label = rec.get("Time", {}).get("Label")
        year = rec.get("Time", {}).get("Year")
        month = rec.get("Time", {}).get("Month")
        month_name = rec.get("Time", {}).get("Month Name")

        stats = rec.get("Statistics", {})

        delays = stats.get("# of Delays", {})
        carriers_info = stats.get("Carriers", {})
        flights = stats.get("Flights", {})
        minutes = stats.get("Minutes Delayed", {})

        row = {
            "airport_code": airport_code,
            "airport_name": airport_name,
            "time_label": time_label,
            "year": year,
            "month": month,
            "month_name": month_name,
            # delays
            "delay_carrier_count": delays.get("Carrier"),
            "delay_late_aircraft_count": delays.get("Late Aircraft"),
            "delay_national_system_count": delays.get("National Aviation System"),
            "delay_security_count": delays.get("Security"),
            "delay_weather_count": delays.get("Weather"),
            # carriers
            "carriers_names": carriers_info.get("Names"),
            "carriers_total": carriers_info.get("Total"),
            # flights
            "flights_cancelled": flights.get("Cancelled"),
            "flights_delayed": flights.get("Delayed"),
            "flights_diverted": flights.get("Diverted"),
            "flights_on_time": flights.get("On Time"),
            "flights_total": flights.get("Total"),
            # minutes delayed
            "minutes_carrier": minutes.get("Carrier"),
            "minutes_late_aircraft": minutes.get("Late Aircraft"),
            "minutes_national_system": minutes.get("National Aviation System"),
            "minutes_security": minutes.get("Security"),
            "minutes_total": minutes.get("Total"),
            "minutes_weather": minutes.get("Weather"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df
