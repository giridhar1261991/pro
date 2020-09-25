
import entity_loader as el
import timesheet_generator as tg
import fetch_timesheet_entries as fte

def handler(event, context):
    # Below handler will initiate data extraction from Planview
    el.handler(event, context)
    
    # Below handler will calculate efforts spent on product by active SE teams 
    # and populate timesheet in planview
    tg.handler(event, context)

    # Below handler extract timesheet entries for is_reportable=true departments and 
    # create time spend report by portfolio, product, tasks and investment type
    fte.handler(event, context)

if __name__ == "__main__":
    handler(None, None)
