WRITINGCRITERIAS:list[map] = [
        {'price_change_percent': -5,'oi_change_percent': 15,'start_time': (9,15,30),'end_time': (11,30,30)},
        {'price_change_percent': -10,'oi_change_percent': 25,'start_time': (11,30,30),'end_time': (12,30,30)},
        {'price_change_percent': -20,'oi_change_percent': 50,'start_time': (12,30,30),'end_time': (15,30,00)}
    ]
UNWINDINGCRITERIAS:list[map] = [
    {'price_change_percent': 5,'oi_change_percent': -15,'start_time': (9,15,30),'end_time': (11,30,30)},
    {'price_change_percent': 10,'oi_change_percent': -25,'start_time': (11,30,30),'end_time': (12,30,30)},
    {'price_change_percent': 20,'oi_change_percent': -50,'start_time': (12,30,30),'end_time': (15,30,00)}
]
CRITERIASTYPES:list[list[map]] = [WRITINGCRITERIAS, UNWINDINGCRITERIAS]