import time
from datetime import datetime, timedelta
from numpy import dtype
from tzlocal import get_localzone
from xml.dom import ValidationErr
from dateutil import parser, tz
import csv
import os


class obsDateRfcHandler:
    def __init__(self, dt):
        if isinstance(dt, datetime):
            self.obs_dt = dt
        else:
            self.obs_dt = parser.parse(dt)  
        self.out_type = 'rfc'
    def _str( dt1):
        try:
            rfc_time = dt1.strftime("%a, %b %d %Y %-H:%M:%S %z")
        except:
            rfc_time = dt1.strftime("%a, %b %d %Y %#H:%M:%S %z")
            
        return rfc_time        
    def emit( dt1):
        return obsDateRfcHandler._str(dt1)
    def __repr__(self):
        return obsDateRfcHandler._str(self.obs_dt)
        # rfc_time = self.obs_dt.strftime("%a, %b %d %Y %I:%M:%S %z")
        #return rfc_time

class obsDateRegHandler:
    def __init__(self,dt):
        #print\("reg date: ", dt )
        dt1 = dt[-4:]
        dt1 = dt1.strip()
        #print("dt1: ", dt1 )
        self.cannicol_tz = None
        #print\("td1: ", dt1, "len:", len(dt1) )
        self.my_tz = tz.gettz(dt1)
        #print\("regdate+tz0:", self.my_tz)
        if str(self.my_tz).find('local') > 0:
             self.my_tz = None
        #print\("regdate+tz1:", self.my_tz)
        if self.my_tz is None:
            self.my_tz = self._wiki_tz_search( dt1 )
            if self.my_tz is None:
                self.cannicol_tz = None
            else:
                self.cannicol_tz = dt1
        else:
            self.cannicol_tz = self.my_tz
        # #print\(parser.parse(dt, tzinfos={dt[-3:]: my_tz}))
        #self.obs_dt = parser.parse(dt)
        # try upper case - Guam - ChST.
        try:
            # print("trying wihtout upper")
            self.obs_dt = parser.parse(dt, tzinfos={dt1 : self.my_tz})
        except:
            # print("now upper")
            # print({dt1:self.my_tz})
            self.obs_dt = parser.parse(dt.upper(), tzinfos={dt1.upper(): self.my_tz})
        #print\("regdate+tz:", self.my_tz)
        #print\("regdate+obs:", self.obs_dt)
        self.out_type = 'reg'
    def _str ( dt1 ):
        try:
            reg_time = dt1.strftime("%b %d %Y, %-I:%M:%S %p %Z")
        except:
            reg_time = dt1.strftime("%b %d %Y, %#I:%M:%S %p %Z")        
        reg_time = reg_time.replace("PM", "pm")
        reg_time = reg_time.replace("AM", "am")
        return reg_time
    def _str2( dt1, tz_txt):
        try:
            reg_time = dt1.strftime("%b %d %Y, %-I:%M:%S %p")
        except:
            reg_time = dt1.strftime("%b %d %Y, %#I:%M:%S %p")        
        reg_time = reg_time.replace("PM", "pm")
        reg_time = reg_time.replace("AM", "am")
        if isinstance(tz_txt, tz.tzfile):
            try:
                reg_time = dt1.strftime("%b %d %Y, %-I:%M:%S %p %Z")
            except:
                reg_time = dt1.strftime("%b %d %Y, %#I:%M:%S %p %Z")   
            return reg_time
            
        return reg_time + " " + str(tz_txt)
    def emit(  dt1):
        return obsDateRegHandler._str( dt1)
    def __repr__(self):
        #print( "__repr__", self.obs_dt )
        #print( self.cannicol_tz)
        #self.obs_dt = self.obs_dt.replace( tzinfo = tz.tzstr(self.cannicol_tz))
        #print( "cannicol: ", self.cannicol_tz)
        if self.cannicol_tz is None:
            return obsDateRegHandler._str( self.obs_dt)
        else: 
            return obsDateRegHandler._str2( self.obs_dt, self.cannicol_tz)
    def _wiki_tz_search(self, dt1 ):
        # don't search for timezone
        if dt1.lower() == 'pm' or dt1.lower() == 'am':
            return None
        if dt1.isdecimal():
            return None
        dt2 = dt1.upper()
        #print\("tzserach")
        #printi\("get: ", timezone_cache.get(dt2), " ", dt2 )
        if timezone_cache.get(dt2) is not  None:
           #\print("cache hit")
           return timezone_cache[dt2]
        with open(ObsDate.obs_time_dir + 'timezone_table_wiki.csv') as csvfile:
            #print\("open csv" )
            tzreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in tzreader:
                # #print\( "row:", row )
                if row[0].startswith(dt2):
                    timezone_cache[dt2] = tz.gettz(row[2])
                    return tz.gettz(row[2])
                


class obsDateDtHandler:
    def __init__(self, dt):
        self.obs_dt = dt
        self.out_type = 'dt' 
    def emit( dt1):
        return str(dt1)
    def __repr__(self):
        return str(self.obs_dt)

class obsExcelDtHandler:
    def __init__(self, dt):
        self.obs_dt = dt
        self.out_type = 'Excel'
        if isinstance(dt, str):
            self.obs_dt = parser.parse(dt)
    def emit( dt1 ):
        return str(dt1)
    def __repr__(self):
        return str(self.obs_dt.strftime('%x %X'))

class ObsCache(dict):

    def __init__(self, maxsize=None):
        super().__init__()
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        #print\("cache size()", len(self ))
        if self.maxsize is not None and len(self) > self.maxsize:
            self.popitem(last=False)

    def get(self, key, default=None):
        #print("self: ", super().get(key, False ) )
        try:
          if super().get(key, False):
            return self[key]
          else:
            return default
        except:
          return False
        
            

class ObsDate():
    obs_time_dir = os.getcwd() + os.sep
    #timezone_cache = ObsCache(  maxsize = 4 )
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
            self.day = self.handler.obs_dt.day
            self.tzinfo = self.handler.obs_dt.tzinfo
            try:
                self.cannicol_tz = self.handler.cannicol_tz
            except:
                self.cannicol_tz = None
        else:
            print(f"error:  ObsDate invalid type {dt}")
        #print\("tzinfo: ", self.tzinfo )
        #print\("tzinfo2: ",type(self.tzinfo))
        #print(self.tzinfo.__dir__())
        #if str(self.tzinfo).find('local') > 0:
         #   print("tzinfo3: ", self.tzinfo.tzname())


    def strftime(self, fstr):
        return self.handler.obs_dt.strftime(fstr)
    
    def replace( self, **kwargs):
        return ObsDate(self.handler.obs_dt.replace(**kwargs))
    
    def __sub__(self, other):
        if isinstance( other, ObsDate):
            return self.handler.obs_dt - other.handler.obs_dt
        else:
            return self.handler.obs_dt - other
    
    def __add__(self, other):
        if isinstance(other,ObsDate):
            return self.handler.obs_dt + other.handler.obs_dt
        else:
            return self.handler.obs_dt + other
    
    def now():
        local = get_localzone()
        return ObsDate(datetime.now(local))
    
    def local_now(self):
        return ObsDate(datetime.now(self.tzinfo))
    
    def local_now_reg( self):
        dt1 = datetime.now(self.tzinfo)
        if str(self.tzinfo).find('local') > 0:
            return ObsDate( dt1 )
        try:
            reg_time = dt1.strftime("%b %d %Y, %-I:%M:%S %p")
        except:
            reg_time = dt1.strftime("%b %d %Y, %#I:%M:%S %p")
            
        if isinstance(self.cannicol_tz, tz.tzfile):
            try:
                reg_time = dt1.strftime("%b %d %Y, %-I:%M:%S %p %Z")
            except:
                reg_time = dt1.strftime("%b %d %Y, %#I:%M:%S %p %Z")   
            return ObsDate(reg_time)
        reg_time = reg_time + " " + str(self.cannicol_tz)
        return ObsDate( reg_time)
        
    
    
    
    def date(self):
        return self.handler.obs_dt.date()
    
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
        if obs_fmt in ['dt', 'rfc', 'reg', 'excel']:
            if obs_fmt == 'dt':
                self.handler = obsDateDtHandler(self.handler.obs_dt)
            if obs_fmt == 'rfc':
                rfc_dt = obsDateRfcHandler.emit( self.handler.obs_dt)
                self.handler = obsDateRfcHandler(rfc_dt)
            if obs_fmt == 'reg':
                reg_dt = obsDateRegHandler.emit(self.handler.obs_dt)
                self.handler = obsDateRegHandler(reg_dt)
            if obs_fmt == 'excel':
                xls_dt = obsExcelDtHandler.emit(self.handler.obs_dt)
                self.handler = obsExcelDtHandler(xls_dt)
        else:
            print(f"Not supportted out_type: {obs_fmt}")
            raise ValueError
    


timezone_cache = ObsCache(  maxsize = 4 )


if __name__ == "__main__":
    # this is a internal test script if invoked as main
    obs_time = "Nov 16 2021, 11:52 am EST"
    obs_time_rfc = "Tue, 16 Nov 2021 11:52:00 -0500"
    obs_time_edt = "Wed, Mar 12 2023, 04:52 am EDT"

    t1 = ObsDate(obs_time)
    t2 = ObsDate(obs_time_rfc)
    t3 = ObsDate(obs_time_edt)

    print(t1)
    print(t2)
    print(t3)

    print("t3, localnow")
    print(t3.local_now_reg())
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
   
    t3.emit_type('rfc')
    print(t3)
    print(ObsDate(str(t3))) 
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
    
    td1 = timedelta(minutes=10)
    
    print("timedelta: ", now2 + td1)
    
    print("timedelta2:", now2 - td1)
    
    now2.emit_type("excel")
    
    print("excel compatbile dates")
    
    now2.add_one_hour()
    
    print(f"now2: {now2}")
    
    print("""
          adding 10 minutes
          """)    
    
    td1 = timedelta(minutes=10)
    
    print("timedelta: ", now2 + td1)
    
    print("timedelta2:", now2 - td1)
    
    
    td2 = ObsDate("6/28/2022 17:40")
    
    print("td2:", td2)
    
    print("""
          Excel tsts
          """)
    
    td2.emit_type("excel")
    
    print("td2(excel:)", td2)
    

    print("testing date: " ,  td2.date())
    
    print("Hawaii tests: ")
    
    print("""
          Add one hour
          print time with local timezone
          """)
    
    td_hawaii = ObsDate("Nov 20 2022, 7:54 pm HST")
    
    print(td_hawaii)
    
    td_hawaii.add_one_hour()
    
    print(td_hawaii)
    
    print(td_hawaii.tzinfo)
    
    print(datetime.now(td_hawaii.tzinfo))
    
    print(td_hawaii.local_now())
    print(td_hawaii.local_now_reg())
    
    print(td_hawaii)
    
    guam_str = "Nov 23 2022, 1:54 am ChST"
    try:
        test_guam = ObsDate(guam_str)
    except:
        print(" must do upper case on Guam")
    td_guam = ObsDate(guam_str.upper())
    
    print(td_guam)
    
    print( td_guam.local_now() )
    print( td_guam.local_now_reg())
    
    print( td_guam.tzinfo)
  
    my_cache = ObsCache( maxsize = 4 )
    my_cache['EDT'] = 2
    print(my_cache['EDT']) 
    print( my_cache.get('jon')) 
    print( my_cache.get('EDT'))
    print( my_cache['EDT']) 

