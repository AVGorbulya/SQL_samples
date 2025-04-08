-- Активные, новые, спящие, проснувшиеся
select makeDate(year_int, month_int, 1) as date,
        year_int,
        month_int,
        status,
        source,
        smb_type,
        client_type,
        uniqExact(user_id) as users
    from
        (
        select user_id,
            source,
            smb_type,
            client_type,
            first_ad_month,
            ads_months,
            month_year_n,
            toInt32(month_n) as month_int,
            toInt32(year_n) as year_int,
            month_int-1 as prev_month_int,
            if(prev_month_int < 10, concat('0', toString(prev_month_int)), toString(prev_month_int)) as prev_month_string,
            multiIf(month_year_n == first_ad_month, 'new',
                    has(ads_months, month_year_n) == 0 and multiIf(
                                                                month_year_n == '01-2022', has(ads_months, '12-2021'),
                                                                month_year_n == '01-2023', has(ads_months, '12-2022'), 
                                                                month_year_n == '01-2024', has(ads_months, '12-2023'), 
                                                                has(ads_months, concat(prev_month_string, '-', year_n))) == 1, 'sleeping',
                    has(ads_months, month_year_n) and multiIf(
                                                            month_year_n == '01-2022', has(ads_months, '12-2021'),
                                                            month_year_n == '01-2023', has(ads_months, '12-2022'), 
                                                            month_year_n == '01-2024', has(ads_months, '12-2023'), 
                                                            has(ads_months, concat(prev_month_string, '-', year_n))) == 0, 'reactive',
                    has(ads_months, month_year_n), 'active', 
                    '') as status
        from
            (
            select user_id,
                if(dictGet('package', 'is_vkads', toUInt64(package_id))=1, 'VKads', 'MT') as source,
                dictGetString('user', 'smb', toUInt64(user_id)) as smb_type,
                if(dictGet('user', 'agency_user_id', toUInt64(user_id)) > 0, 'agency', 'direct') as client_type,
                formatDateTime(min(date), '%m-%Y') as first_ad_month,
                groupArray(distinct formatDateTime(date, '%m-%Y')) as ads_months,
                arrayJoin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']) as month_n,
                arrayJoin([toString(prev_year-1), toString(prev_year), toString(cur_year)]) as year_n,
                concat(month_n, '-', year_n) as month_year_n
            from default.banner_day
            where date >= toDate('2021-01-01') and smb_type in  ('smb', 'undefined')
                and a_amount > 0
                and user_id != 0
            GROUP BY user_id, source, smb_type, client_type
            )
        where status != ''
            and makeDate(year_int, month_int, 1) <= toStartOfMonth(today())
        )
    group by year_int, month_int, status, smb_type, client_type, source
    order by year_int, month_int, status, smb_type, client_type, source