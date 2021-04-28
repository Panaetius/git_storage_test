from test import Activity

all_activities = Activity.load_all()

print(f"Loaded {len(all_activities)} activities")

print(all_activities[0].id_)
