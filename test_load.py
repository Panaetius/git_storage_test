from test import Activity

all_activities = Activity.load_all()
print(all_activities)

print(all_activities[0].id_)