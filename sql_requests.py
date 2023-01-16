query = '''
    select
        date,
        time_window,
        users_feed,
        views,
        likes,
        ctr,
        users_mes,
        mes
    from
        (
        select
            toDate(time) as date,
            max(time) as time_window,
            uniqExact(user_id) as users_feed,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(likes / views, 3) as ctr
        from
            ( 
            select
                time,
                user_id,
                action
            from *таблица в базе данных*
            where time between (now() - interval 3 day - interval 15 minute) and (now())
            )
        where toTime(time) between (toTime(now()) - interval 15 minute) and toTime(now())
        group by date
        ) l

    join

        (
        select
            toDate(time) as date,
            uniqExact(user_id) as users_mes,
            count(user_id) as mes
        from
            ( 
            select
                time,
                user_id
            from *таблица в базе данных*
            where time between (now() - interval 3 day - interval 15 minute) and (now())
            )
        where toTime(time) between (toTime(now()) - interval 15 minute) and toTime(now())
        group by date
        ) r
    on l.date = r.date
    '''

query_anomaly_15_min = '''
    select 
        time_window,
        users_feed,
        views,
        likes,
        ctr,
        users_mes,
        mes
    from 
        (
        select 
            toStartOfFifteenMinutes(time) as time_window,
            uniqExact(user_id) as users_feed,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(likes / views, 3) as ctr
        from    
            (
            select
                time,
                user_id,
                action
            from *таблица в базе данных*
            where time between toStartOfDay(now() - interval 3 day) and now()
              and toTime(time) < toTime(now()) 
            )
        group by toStartOfFifteenMinutes(time)
        order by time_window 
        ) l

    join 

        (
        select 
            toStartOfFifteenMinutes(time) as time_window,
            uniqExact(user_id) as users_mes,
            count(user_id) as mes
        from    
            (
            select
                time,
                user_id
            from *таблица в базе данных*
            where time between toStartOfDay(now() - interval 3 day) and now()
              and toTime(time) < toTime(now()) 
            )
        group by toStartOfFifteenMinutes(time)
        order by time_window 
        ) r
    on l.time_window = r.time_window
'''

query_anomaly_5_min = '''
    select 
        time_window,
        users_feed,
        views,
        likes,
        ctr,
        users_mes,
        mes
    from 
        (
        select 
            toStartOfFiveMinute(time) as time_window,
            uniqExact(user_id) as users_feed,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            round(likes / views, 3) as ctr
        from    
            (
            select
                time,
                user_id,
                action
            from *таблица в базе данных* 
            where time between toStartOfDay(now() - interval 3 day) and now()
              and toTime(time) < toTime(now()) 
            )
        group by toStartOfFiveMinute(time)
        order by time_window 
        ) l

    join 

        (
        select 
            toStartOfFiveMinute(time) as time_window,
            uniqExact(user_id) as users_mes,
            count(user_id) as mes
        from    
            (
            select
                time,
                user_id
            from *таблица в базе данных*
            where time between toStartOfDay(now() - interval 3 day) and now()
              and toTime(time) < toTime(now()) 
            )
        group by toStartOfFiveMinute(time)
        order by time_window 
        ) r
    on l.time_window = r.time_window
'''