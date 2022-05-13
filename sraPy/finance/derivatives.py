import pendulum
from dateutil.relativedelta import *
from dateutil.rrule import *
from exchange_calendars.exchange_calendar_xnys import XNYSExchangeCalendar as nyseCal


lstMonthlyExpiration = []

months = 12 + (12 - pendulum.today('America/Chicago').month)
lstThirdFriday = list(rrule(MONTHLY, count = months, byweekday = FR(3), dtstart = pendulum.now().date()))

for date in lstThirdFriday:
    validContractExpiry = nyseCal().date_to_session_label(date=date, direction="previous")
    lstMonthlyExpiration.append(validContractExpiry.strftime("%Y-%m-%d"))

# sort months
lstMonthlyExpiration = sorted(list(set(lstMonthlyExpiration)))

strMonthlyExpiration = "','".join(lstMonthlyExpiration)
