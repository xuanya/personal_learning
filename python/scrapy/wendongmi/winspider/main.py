from scrapy import cmdline

dates = [('2017-01-01', '2017-03-31'), 
        ('2017-04-01', '2017-06-30'), 
        ('2017-07-01', '2017-09-30'), 
        ('2017-10-01', '2017-12-31'), 
        ('2018-01-01', '2018-03-31')]
# 深交所数据爬取
for s, e in dates:
    cmd = 'scrapy crawl dongmi_sz --logfile=./logs/logs_{}.log -a date_from={} -a data_to={}'.format(s, s, e)
    print(cmd)
    # cmdline.execute(cmd.split())

# 上交所数据爬取
sh_dates = [('2017', '2017-01-01', '2017-03-31'), 
        ('2017', '2017-04-01', '2017-06-30'), 
        ('2017', '2017-07-01', '2017-09-30'), 
        ('2017', '2017-10-01', '2017-12-31'), 
        ('2018', '2018-01-01', '2018-03-31')]

for y, s, e in sh_dates:
    cmd = 'scrapy crawl dongmi_sh --logfile=./logs/sh_logs_{}.log -a year={} -a sdate={} -a edate={}'.format(s, y, s, e)
    print(cmd)
    # cmdline.execute(cmd.split())

