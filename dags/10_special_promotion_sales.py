from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable
from airflow.sdk.bases.operator import chain

# Define special promotion dates and details
promotion_schedule = {
    "2026-01-01": {
        "name": "New Year's Sale",
        "discount": "400%",
        "description": "Special New Year's offer - Save on all products"
    },
    "2026-02-14": {
        "name": "Valentine's Day Sale",
        "discount": "30%",
        "description": "Special Valentine's Day offer - Save on all products"
    },
    "2026-02-23": {
        "name": "Presidents Day Sale",
        "discount": "25%",
        "description": "Presidents Day Special - Huge discounts on top items"
    },
    "2026-02-26": {
        "name": "Mid-Winter Clearance",
        "discount": "40%",
        "description": "Clear out winter items with massive savings"
    },
}

# Define customer database for Canada and America
customers_database = {
    "Canada": [
        {"id": 1, "name": "John Smith", "email": "john.smith@email.ca", "city": "Toronto", "country": "Canada"},
        {"id": 2, "name": "Sarah Johnson", "email": "sarah.j@email.ca", "city": "Vancouver", "country": "Canada"},
        {"id": 3, "name": "Mike Wilson", "email": "mike.w@email.ca", "city": "Montreal", "country": "Canada"},
        {"id": 4, "name": "Emma Brown", "email": "emma.b@email.ca", "city": "Calgary", "country": "Canada"},
        {"id": 5, "name": "Alex Lee", "email": "alex.lee@email.ca", "city": "Ottawa", "country": "Canada"},
    ],
    "USA": [
        {"id": 6, "name": "Tom Anderson", "email": "tom.a@email.com", "city": "New York", "country": "USA"},
        {"id": 7, "name": "Lisa Davis", "email": "lisa.d@email.com", "city": "Los Angeles", "country": "USA"},
        {"id": 8, "name": "Chris Martin", "email": "chris.m@email.com", "city": "Chicago", "country": "USA"},
        {"id": 9, "name": "Jennifer White", "email": "jen.w@email.com", "city": "Houston", "country": "USA"},
        {"id": 10, "name": "Daniel Green", "email": "daniel.g@email.com", "city": "Phoenix", "country": "USA"},
    ]
}

special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=1, day=1, tz="Canada/Eastern"),    # New Year's Day
        datetime(year=2026, month=2, day=14, tz="Canada/Eastern"),   # Valentine's Day
        datetime(year=2026, month=2, day=23, tz="America/Chicago"),  # Presidents Day
        datetime(year=2026, month=2, day=26, tz="America/New_York"), # Mid-Winter Clearance
    ]
)

@dag(
    schedule=special_dates,
    start_date=datetime(year=2025, month=12, day=30, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True,
    dag_id="special_promotion_sales_dag"
)
def special_promotion_sales_dag():

    @task.python
    def identify_promotion(**kwargs):
        """Identify which promotion is running"""
        event_date = kwargs['logical_date']
        event_date_str = event_date.strftime("%Y-%m-%d")
        
        print(f"\n{'='*80}")
        print(f"PROMOTION EVENT DETECTED - {event_date_str}")
        print(f"{'='*80}")
        
        promotion_details = promotion_schedule.get(event_date_str)
        
        if promotion_details:
            print(f"✓ Promotion Found!")
            print(f"  Name: {promotion_details['name']}")
            print(f"  Discount: {promotion_details['discount']}")
            print(f"  Description: {promotion_details['description']}")
            return promotion_details
        else:
            print(f"✗ No promotion found for {event_date_str}")
            return None

    @task.python
    def get_canada_customers():
        """Retrieve all customers from Canada"""
        canada_customers = customers_database.get("Canada", [])
        
        print(f"\n{'='*80}")
        print(f"FETCHING CANADA CUSTOMERS")
        print(f"{'='*80}")
        print(f"Total Canada Customers: {len(canada_customers)}")
        
        for customer in canada_customers:
            print(f"  - {customer['name']} ({customer['city']}) - {customer['email']}")
        
        return canada_customers

    @task.python
    def get_usa_customers():
        """Retrieve all customers from USA"""
        usa_customers = customers_database.get("USA", [])
        
        print(f"\n{'='*80}")
        print(f"FETCHING USA CUSTOMERS")
        print(f"{'='*80}")
        print(f"Total USA Customers: {len(usa_customers)}")
        
        for customer in usa_customers:
            print(f"  - {customer['name']} ({customer['city']}) - {customer['email']}")
        
        return usa_customers

    @task.python
    def send_promotions_canada(**kwargs):
        """Send promotions to all Canadian customers"""
        ti = kwargs['ti']
        
        promotion_details = ti.xcom_pull(task_ids='identify_promotion')
        canada_customers = ti.xcom_pull(task_ids='get_canada_customers')
        
        if not promotion_details or not canada_customers:
            print("Skipping - No promotion or customers found")
            return []
        
        print(f"\n{'='*80}")
        print(f"📧 SENDING PROMOTIONS TO CANADIAN CUSTOMERS")
        print(f"{'='*80}")
        
        sent_emails = []
        for customer in canada_customers:
            email_record = {
                "customer_id": customer['id'],
                "name": customer['name'],
                "email": customer['email'],
                "country": "Canada",
                "promotion": promotion_details['name'],
                "discount": promotion_details['discount'],
                "status": "sent"
            }
            
            print(f"\n📬 Email sent to: {customer['name']}")
            print(f"   Email: {customer['email']}")
            print(f"   Promotion: {promotion_details['name']}")
            print(f"   Discount: {promotion_details['discount']}")
            print(f"   Details: {promotion_details['description']}")
            
            sent_emails.append(email_record)
        
        print(f"\n{'='*80}")
        print(f"✓ Total emails sent to Canada: {len(sent_emails)}")
        print(f"{'='*80}\n")
        
        ti.xcom_push(key='canada_emails', value=sent_emails)
        return sent_emails

    @task.python
    def send_promotions_usa(**kwargs):
        """Send promotions to all USA customers"""
        ti = kwargs['ti']
        
        promotion_details = ti.xcom_pull(task_ids='identify_promotion')
        usa_customers = ti.xcom_pull(task_ids='get_usa_customers')
        
        if not promotion_details or not usa_customers:
            print("Skipping - No promotion or customers found")
            return []
        
        print(f"\n{'='*80}")
        print(f"📧 SENDING PROMOTIONS TO USA CUSTOMERS")
        print(f"{'='*80}")
        
        sent_emails = []
        for customer in usa_customers:
            email_record = {
                "customer_id": customer['id'],
                "name": customer['name'],
                "email": customer['email'],
                "country": "USA",
                "promotion": promotion_details['name'],
                "discount": promotion_details['discount'],
                "status": "sent"
            }
            
            print(f"\n📬 Email sent to: {customer['name']}")
            print(f"   Email: {customer['email']}")
            print(f"   Promotion: {promotion_details['name']}")
            print(f"   Discount: {promotion_details['discount']}")
            print(f"   Details: {promotion_details['description']}")
            
            sent_emails.append(email_record)
        
        print(f"\n{'='*80}")
        print(f"✓ Total emails sent to USA: {len(sent_emails)}")
        print(f"{'='*80}\n")
        
        ti.xcom_push(key='usa_emails', value=sent_emails)
        return sent_emails

    @task.python
    def promotion_summary_report(**kwargs):
        """Generate summary report of promotion campaign"""
        ti = kwargs['ti']
        
        promotion_details = ti.xcom_pull(task_ids='identify_promotion')
        canada_emails = ti.xcom_pull(task_ids='send_promotions_canada', key='canada_emails')
        usa_emails = ti.xcom_pull(task_ids='send_promotions_usa', key='usa_emails')
        
        total_canada = len(canada_emails) if canada_emails else 0
        total_usa = len(usa_emails) if usa_emails else 0
        total_all = total_canada + total_usa
        
        print(f"\n{'='*80}")
        print(f"📊 PROMOTION CAMPAIGN SUMMARY REPORT")
        print(f"{'='*80}")
        print(f"\nPromotion: {promotion_details['name']}")
        print(f"Discount: {promotion_details['discount']}")
        print(f"Description: {promotion_details['description']}")
        print(f"\n{'─'*80}")
        print(f"Emails Sent Summary:")
        print(f"  Canada: {total_canada} customers")
        print(f"  USA: {total_usa} customers")
        print(f"  TOTAL: {total_all} customers across North America")
        print(f"{'─'*80}")
        print(f"Status: ✓ Campaign Successfully Executed")
        print(f"{'='*80}\n")
        
        report = {
            "promotion": promotion_details['name'],
            "discount": promotion_details['discount'],
            "canada_emails_sent": total_canada,
            "usa_emails_sent": total_usa,
            "total_emails_sent": total_all,
            "status": "completed"
        }
        
        ti.xcom_push(key='return_value', value=report)
        return report

    # Create task instances
    identify_task = identify_promotion()
    canada_customers_task = get_canada_customers()
    usa_customers_task = get_usa_customers()
    send_canada_task = send_promotions_canada()
    send_usa_task = send_promotions_usa()
    summary_task = promotion_summary_report()
    
    # Set task dependencies - parallel processing for both regions
    identify_task >> canada_customers_task >> send_canada_task >> summary_task
    identify_task >> usa_customers_task >> send_usa_task >> summary_task

# Generate/Initiate the DAG
special_promotion_sales_dag()