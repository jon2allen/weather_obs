import time
from datetime import datetime, timedelta
from tzlocal import get_localzone
from xml.dom import ValidationErr
from dateutil import parser, tz


class obsDateRfcHandler:
    def __init__(self, dt):
        self.obs_dt = parser.parse(dt)
        self.out_type = 'rfc'
    def _str( dt1):
        rfc_time = dt1.strftime("%a, %b %d %Y %H:%M:%S %z")
        return rfc_time        
    def emit( dt1):
        return obsDateRfcHandler._str(dt1)
    def __repr__(self):
        return obsDateRfcHandler._str(self.obs_dt)
        # rfc_time = self.obs_dt.strftime("%a, %b %d %Y %I:%M:%S %z")
        #return rfc_time

class obsDateRegHandler:
    def __init__(self,dt):
        my_tz = tz.gettz(dt[-3:])
        # print(my_tz)
        # print(parser.parse(dt, tzinfos={dt[-3:]: my_tz}))
        self.obs_dt = parser.parse(dt, tzinfos={dt[-3:]: my_tz})
        self.out_type = 'reg'
    def _str( dt1 ):
        reg_time = dt1.strftime("%b %d %Y, %I:%M:%S %p %Z")
        reg_time = reg_time.replace("PM", "pm")
        reg_time = reg_time.replace("AM", "am")
        return reg_time
    def emit( dt1):
        return obsDateRegHandler._str( dt1)
    def __repr__(self):
        return obsDateRegHandler._str( self.obs_dt)


class obsDateDtHandler:
    def __init__(self, dt):
        self.obs_dt = dt
        self.out_type = 'dt' 
    def emit( dt1):
        return str(dt1)
    def __repr__(self):
        return str(self.obs_dt)

        
            

class ObsDate():
    def __init__(self, dt):
        if isinstance(dt, datetime):
            self.handler = obsDateDtHandler(dt)
        if isinstance(dt, str):
            if '-' in dt:
                self.handler = obsDateRfcHandler(dt)
            else:
                self.handler = obsDateRegHandler(dt)
        if self.handler:
            self.seconds = self.handler.obs_dt.second
            self.minute = self.handler.obs_dt.minute
            self.hour = self.handler.obs_dt.hour
            self.year = self.handler.obs_dt.year
            self.month = self.handler.obs_dt.month
        else:
            print(f"error:  ObsDate invalid type {dt}")

    def strftime(self, fstr):
        return self.handler.obs_dt.strftime(fstr)
    
    def replace( self, **kwargs):
        return ObsDate(self.handler.obs_dt.replace(**kwargs))
    
    def __sub__(self, other):
        return self.handler.obs_dt - other.handler.obs_dt
    
    def __add__(self, other):
        return self.handler.obs_dt + other.handler.obs_dt
    
    def now():
        local = get_localzone()
        return ObsDate(datetime.now(local))
    def get_datetime(self):
        return self.handler.obs_dt        
    def add_one_hour(self):
        self.add_multi_hours(1)

    def add_multi_hours(self, num_hours):
        my_obs_dt = self.handler.obs_dt
        self.handler.obs_dt = self._add_hours(num_hours, my_obs_dt)

    def _add_hours(self, num_hours, date_time_var):
        dt_t = timedelta(hours=num_hours)
        date_time_var = date_time_var + dt_t
        return date_time_var
    
    def is_future( self, new_obs_dt):
        # must be greater than one hour ahead 
        # make into Naive dates
        # only works for same timezone.
        cmp1 = self.handler.obs_dt.replace(tzinfo=None)
        target_obs_dt = self._add_hours( 1, new_obs_dt )
        cmp2 = target_obs_dt.replace(tzinfo=None)
        if cmp1 > cmp2:
        #if new_obs_dt > target_obs_dt:
            return True
        else:
            return False

    def __repr__(self):
        return self.handler.__repr__()
 
    def emit_type(self, obs_fmt):
        if obs_fmt in ['dt', 'rfc', 'reg']:
            if obs_fmt == 'dt':
                self.handler = obsDateDtHandler(self.handler.obs_dt)
            if obs_fmt == 'rfc':
                rfc_dt = obsDateRfcHandler.emit( self.handler.obs_dt)
                self.handler = obsDateRfcHandler(rfc_dt)
            if obs_fmt == 'reg':
                reg_dt = obsDateRegHandler.emit(self.handler.obs_dt)
                self.handler = obsDateRegHandler(reg_dt)
        else:
            print(f"Not supportted out_type: {obs_fmt}")
            raise ValueError
    




if __name__ == "__main__":
    # this is a internal test script if invoked as main
    obs_time = "Nov 16 2021, 11:52 am EST"
    obs_time_rfc = "Tue, 16 Nov 2021 11:52:00 -0500"

    t1 = ObsDate(obs_time)
    t2 = ObsDate(obs_time_rfc)

    print(t1)
    print(t2)

    for x in range(10):
        t1.add_one_hour()
        print(t1)

    for x in range(10):
        t2.add_one_hour()
        print(t2)

    t2.add_one_hour()
    print(type(t2))
    t2.emit_type('reg')
    print(t2)
    try:
        t2.emit_type('rfc2')
    except:
        print("error caught")
    t2.emit_type('rfc')
    print(t2)
    t2.emit_type('reg')
    print(t2)
    t2.add_multi_hours(int(-26))

    print(t2)
    
    t2.add_multi_hours(int(28))
    
    print(f"t2: {t2}")
    print(f"t1: {t1}")
    
    print( t2.handler.obs_dt)
    print(t1.handler.obs_dt)

    
    print(t1.is_future( t2.handler.obs_dt))
    print(t2.is_future(t1.handler.obs_dt))
    
    now = ObsDate(datetime.now())
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    my_seconds = (now - midnight).seconds
    print(midnight)
    print(now)
    #my_seconds =   now.seconds - midnight.seconds
    print(f'seconds since midnight {my_seconds}')
    
    now2 = ObsDate.now()
    
    print(now2)
    
    now2.emit_type("rfc")
    
    print(f"now2: {now2}")
    
    now2.emit_type("reg")
    
    now2.add_one_hour()
    
    print(f"now2: {now2}")
    
    