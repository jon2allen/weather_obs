import time
from datetime import datetime, timedelta
from xml.dom import ValidationErr
from dateutil import parser, tz


class ObsDate():
    def __init__(self, dt):
        if isinstance(dt, datetime):
            self.obs_dt = dt
            self.out_type = 'dt'
        if isinstance(dt, str):
            if '-' in dt:
                # print("rfc")
                # print(parser.parse(dt))
                self.obs_dt = parser.parse(dt)
                self.out_type = 'rfc'
            else:
                print("regular")
                # print(parser.parse(dt))
                my_tz = tz.gettz(dt[-3:])
                # print(my_tz)
                # print(parser.parse(dt, tzinfos={dt[-3:]: my_tz}))
                self.obs_dt = parser.parse(dt, tzinfos={dt[-3:]: my_tz})
                self.out_type = 'reg'

    def add_one_hour(self):
        self.add_multi_hours(1)

    def add_multi_hours(self, num_hours):
        self.obs_dt = self._add_hours(num_hours, self.obs_dt)

    def _add_hours(self, num_hours, date_time_var):
        dt_t = timedelta(hours=num_hours)
        date_time_var = date_time_var + dt_t
        return date_time_var
    
    def is_future( self, new_obs_dt):
        # must be greater than one hour ahead 
        target_obs_dt = self._add_hours( 1, self.obs_dt)
        if new_obs_dt > target_obs_dt:
            return True
        else:
            return False

    def __repr__(self):
        if self.out_type == 'dt':
            return str(self.obs_dt)
        if self.out_type == 'rfc':
            rfc_time = self.obs_dt.strftime("%a, %b %d %Y %I:%M:%S %z")
            return rfc_time
        if self.out_type == 'reg':
            # return self.obs_dt.strftime("%a, %M %b %Y %I:%M:%S %P %Z")
            reg_time = self.obs_dt.strftime("%b %d %Y, %I:%M:%S %p %Z")
            reg_time = reg_time.replace("PM", "pm")
            reg_time = reg_time.replace("AM", "am")
            return reg_time

    def emit_type(self, obs_fmt):
        if obs_fmt in ['dt', 'rfc', 'reg']:
            self.out_type = obs_fmt
        else:
            print(f"Not supportted out_type: {obs_fmt}")
            raise ValueError


if __name__ == "__main__":
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
    
    print(t1.is_future( t2.obs_dt))
    print(t2.is_future(t1.obs_dt))
