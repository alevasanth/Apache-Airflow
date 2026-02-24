from  airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

# Define special events with people and event types
special_events_map = {
    "2026-02-05": {"name": "Alice Johnson", "email": "alice@example.com", "event": "Birthday"},
    "2026-02-14": {"name": "Raj Kumar", "email": "raj@example.com", "event": "Anniversary"},
    "2026-02-19": {"name": "Emma Wilson", "email": "emma@example.com", "event": "Birthday"},
    "2026-02-23": {"name": "Michael Chen", "email": "michael@example.com", "event": "Birthday"},
    "2026-02-24": {"name": "Priya Sharma", "email": "priya@example.com", "event": "Anniversary"},
    "2026-02-26": {"name": "David Brown", "email": "david@example.com", "event": "Birthday"},
}

special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=2, day=5, tz="Canada/Eastern"),
        datetime(year=2026, month=2, day=15, tz="Asia/Kolkata"),
        datetime(year=2026, month=2, day=19, tz="Europe/London"),
        datetime(year=2026, month=2, day=23, tz="America/Chicago"),
        datetime(year=2026, month=2, day=25, tz="Asia/Kolkata"),
        datetime(year=2026, month=2, day=26, tz="America/New_York"),
    ]
)

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=1, day=30, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True,
    dag_id="special_events_wishes_dag"
)
def special_events_wishes_dag():

    @task.python
    def identify_event(**kwargs):
        event_date = kwargs['logical_date']
        event_date_str = event_date.strftime("%Y-%m-%d")
        
        print(f"\n{'='*70}")
        print(f"SPECIAL EVENT PROCESSING - {event_date_str}")
        print(f"{'='*70}")
        
        # Find the event details
        event_details = special_events_map.get(event_date_str)
        
        if event_details:
            print(f"Event Found!")
            print(f"  Person: {event_details['name']}")
            print(f"  Email: {event_details['email']}")
            print(f"  Event Type: {event_details['event']}")
            return event_details
        else:
            print(f"No event found for {event_date_str}")
            return None
    
    @task.python
    def send_birthday_wishes(**kwargs):
        ti = kwargs['ti']
        event_details = ti.xcom_pull(task_ids='identify_event')
        
        if not event_details or event_details['event'] != "Birthday":
            print("Skipping - Not a birthday event")
            return
        
        name = event_details['name']
        email = event_details['email']
        
        print(f"\n{'='*50}")
        print(f"🎉 SENDING BIRTHDAY WISHES 🎉")
        print(f"{'='*50}")
        print(f"To: {name} <{email}>")
        print(f"\nDear {name},")
        print(f"Wishing you a wonderful birthday!")
        print(f"Hope this special day brings you joy and happiness.")
        print(f"Have a fantastic year ahead!")
        print(f"\nBest wishes,")
        print(f"Admin Team")
        print(f"{'='*50}\n")
        
        # In production, use email provider like SendGrid/SMTP
        # For now, just logging
        message = {
            "recipient": name,
            "email": email,
            "message_type": "birthday",
            "status": "sent"
        }
        ti.xcom_push(key='birthday_message', value=message)
        return message
    
    @task.python
    def send_anniversary_wishes(**kwargs):
        ti = kwargs['ti']
        event_details = ti.xcom_pull(task_ids='identify_event')
        
        if not event_details or event_details['event'] != "Anniversary":
            print("Skipping - Not an anniversary event")
            return
        
        name = event_details['name']
        email = event_details['email']
        
        print(f"\n{'='*50}")
        print(f"🎊 SENDING ANNIVERSARY WISHES 🎊")
        print(f"{'='*50}")
        print(f"To: {name} <{email}>")
        print(f"\nDear {name},")
        print(f"Celebrating your special milestone!")
        print(f"Thank you for your continued partnership and support.")
        print(f"Looking forward to many more years together!")
        print(f"\nWarm regards,")
        print(f"Admin Team")
        print(f"{'='*50}\n")
        
        message = {
            "recipient": name,
            "email": email,
            "message_type": "anniversary",
            "status": "sent"
        }
        ti.xcom_push(key='anniversary_message', value=message)
        return message
    
    @task.python
    def log_event_summary(**kwargs):
        ti = kwargs['ti']
        event_details = ti.xcom_pull(task_ids='identify_event')
        birthday_msg = ti.xcom_pull(task_ids='send_birthday_wishes', key='birthday_message')
        anniversary_msg = ti.xcom_pull(task_ids='send_anniversary_wishes', key='anniversary_message')
        
        print(f"\n{'='*70}")
        print(f"EVENT SUMMARY")
        print(f"{'='*70}")
        print(f"Event Details: {event_details}")
        print(f"Birthday Messages Sent: {birthday_msg}")
        print(f"Anniversary Messages Sent: {anniversary_msg}")
        print(f"{'='*70}\n")
        
        summary = {
            "event": event_details,
            "birthday_message": birthday_msg,
            "anniversary_message": anniversary_msg
        }
        ti.xcom_push(key='return_value', value=summary)
        return summary

    # Set task dependencies
    event_task = identify_event()
    birthday_task = send_birthday_wishes()
    anniversary_task = send_anniversary_wishes()
    summary_task = log_event_summary()
    
    event_task >> [birthday_task, anniversary_task] >> summary_task

# Generate/Initiate the DAG
special_events_wishes_dag()