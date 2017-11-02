-===============================================================================================================================================
' With statement [Tree structured]....Values of pass_1 gets passed to pass_2 => pass_3 => pass_4 => final query gets all the values from pass_4
-===============================================================================================================================================

WITH pass_1 AS
 (Select distinct dic.VALUATION_DATE,dic.pol_id,iss_DATE dpt_date,iss_date, g_EFF_DT,g_TERM,
      case 
        when abs(to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from g_EFF_DT)-1),'dd-mm-yyyy') - g_EFF_DT) <
             abs(to_date(to_char(iss_date,'dd-mm-')|| extract(year from g_EFF_DT),'dd-mm-yyyy') - g_EFF_DT) then
           case when to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from g_EFF_DT)-1),'dd-mm-yyyy') > iss_date then
                 to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from g_EFF_DT)-1),'dd-mm-yyyy') else iss_date end
        else
            case when to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from g_EFF_DT)),'dd-mm-yyyy') > iss_date then
              to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from g_EFF_DT)),'dd-mm-yyyy') else iss_date end
      end anni_dt,-- anniversary date
      case 
        when to_date(to_char(iss_date,'dd-mm-')|| extract(year from VALUATION_DATE),'dd-mm-yyyy') <= VALUATION_DATE then
           to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from VALUATION_DATE)+1),'dd-mm-yyyy')
        else
           to_date(to_char(iss_date,'dd-mm-')|| to_char(extract(year from VALUATION_DATE)),'dd-mm-yyyy')
      end anni_val_dt, --anniversary dt after valuation date
      first_value(g_TIER) over (partition by pol_id order by g_TIER desc) tier_cnt,  -- break query as tier_cnt = 1 and tier_cnt > 1
      add_months(iss_DATE, g_TERM) excpt_dt,
      case when add_months(iss_DATE, g_TERM) <= valuation_date then 0 else 1 end excpt_flag,
      min_g_cred_rate, g_rate rate,dpt Value, g_TIER 
  from dict dic 
  where 1=1 and dic.valuation_date =to_date('20170630','yyyymmdd') 
 ),
 pass_2 as ( select a.*,
    LAG(add_months(anni_dt,g_TERM), 1, anni_dt) OVER (partition by pol_id order by g_TIER) AS start_dt, 
    add_months(LAG(add_months(anni_dt,g_TERM), 1, anni_dt) OVER (partition by pol_id order by g_TIER),g_TERM) as end_dt, -- add_months(start_dt,g_TERM) as end_dt
    case when valuation_date > -- if valuation_date > end_dt then [expstatus] i.e expired = 0 ; non-expired = 1,
         add_months(LAG(add_months(anni_dt,g_TERM), 1, anni_dt) OVER (partition by pol_id order by g_TIER),g_TERM) then 0 else 1 end expstatus,
    round(months_between(anni_val_dt,iss_date)) valdt_term -- term for all expired     
 from pass_1 a),
 pass_3 as (select a.*,
     case when (abs(g_EFF_DT-iss_Date) < 365) and (tier_cnt = 1) then g_TERM else round(months_between(end_dt,iss_date)) end term, -- normal term for all active ones in all tiers
     sum(expstatus) over (partition by pol_id order by expstatus desc) tot_expiry,
     -- Weighted Average calculatuion begins
     greatest(valuation_date, start_dt) as start_dt2,
     round(end_dt - greatest(valuation_date, start_dt)) days_period,    rate/100 wt_rate,
     power((1+rate/100), (round(end_dt - greatest(valuation_date, start_dt))/365.25)) acc_factor   
 from  pass_2 a),
  pass_4 as (select a.*, 
      sum(case when expstatus = 1 then days_period else 0 end) over (partition by pol_id order by expstatus) sum_period,
      sum(case when expstatus = 1 then acc_factor  else 0 end) over (partition by pol_id order by expstatus) sum_accfactor,
      case when expstatus = 0 then 0 else 1/replace(((sum(case when expstatus = 1 then days_period else 0 end) over (partition by pol_id order by expstatus))/365.25),0,1) end sum_expn,
      sum (g_term) over (partition by pol_id order by pol_id) sum_term  
  from  pass_3 a)
    -- All active pol with only 1 active pol [months between iss date and end_dt]
   select distinct VALUATION_DATE,11 as itrn_no,pol_ID, pol_ID pol_ID_TEXT, EXTRACT(Day FROM dpt_Date) dpt_DAY , 
        EXTRACT(Month FROM dpt_Date) dpt_MONTH, EXTRACT(YEAR FROM dpt_Date) dpt_YEAR,
        dpt_Date, VALUE,case when rate/100 < = min_g_cred_rate then min_g_cred_rate else rate/100 end RATE,
        case when TERM > 1200 then 1200 
         else 
           case when excpt_flag = 0 and tier_cnt = 1 then valdt_term else ceil(TERM) end
         end TERM 
          , tier_cnt , expstatus, tot_expiry, g_TIER, excpt_flag
   from pass_4 a where expstatus = 1 and tot_expiry = 1
   union all
   -- all expired pol or hide pol for 1 active pol [months between iss date and anni_val_dt]
   select distinct VALUATION_DATE,33 as itrn_no,pol_ID, pol_ID pol_ID_TEXT, EXTRACT(Day FROM dpt_Date) dpt_DAY , 
        EXTRACT(Month FROM dpt_Date) dpt_MONTH, EXTRACT(YEAR FROM dpt_Date) dpt_YEAR,
        dpt_Date, VALUE, min_g_cred_rate RATE, valdt_term TERM
        , tier_cnt , expstatus ,tot_expiry, g_TIER, excpt_flag
   FROM pass_4 a where  expstatus = 0 and tot_expiry < 1
  union all
  -- g_tier > 1 and more than one pol non-expired [Weigthed avg]
   select distinct VALUATION_DATE,44 as itrn_no,pol_ID, pol_ID pol_ID_TEXT, EXTRACT(Day FROM dpt_Date) dpt_DAY , 
        EXTRACT(Month FROM dpt_Date) dpt_MONTH, EXTRACT(YEAR FROM dpt_Date) dpt_YEAR,
        dpt_Date, VALUE,round(power(sum_accfactor,sum_expn)-1,5) RATE,sum_term TERM 
        , tier_cnt ,expstatus, tot_expiry, g_TIER, excpt_flag
   FROM pass_4 a where tier_cnt > 1 and tot_expiry > 1 
--SELECT a.* FROM pass_4 a
/*SELECT a.pol_id, iss_date, g_eff_dt, anni_dt, valuation_date, anni_val_dt, start_dt, end_dt,
g_term,  term, valdt_term, min_g_cred_rate, rate,
g_tier, tier_cnt, expstatus, tot_expiry, excpt_dt, excpt_flag  FROM pass_4 a*/


-=====================================================
' With statement [multiple named queries]....
-=====================================================

insert into acrs select  z.* from (
      with 
        gather_data AS ( select a, b, c , d  from acfd a where a.a = 1 and a.b = 100 ) , 
	expected AS ( select count(*) cnt, p, q, r, s from gd GROUP BY p,q,r,s ) ,
        actual AS  ( select count(*) cnt, x, y, z from acr GROUP BY x,y,z )
      select b, c, d, p, q, y, z , sysdate ,  user
        from gather_data a , expected b ,  actual c
       where a.a = b.p(+)
         and c.x = b.p(+)
         and a.a = c.x
         ) z;