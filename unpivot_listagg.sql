-================================================
-' unpivot columns (make columns to rows)
-================================================

select company as company, company_code as company_code, 
    sum(FEED_TYPE_DIFF) as FEED_TYPE_DIFF, 
    sum(DATA_SOURCE_DIFF) as  DATA_SOURCE_DIFF, 
    sum(ISSUE_DATE_DIFF) as ISSUE_DATE_DIFF, 
    sum(SEX_PRIMARY_DIFF) as SEX_PRIMARY_DIFF, 
    sum(TAX_STATUS_DIFF) as  TAX_STATUS_DIFF ,
    sum(PRODUCT_NAME_DIFF) as PRODUCT_NAME_DIFF ,
    sum(PRODUCT_CODE_DIFF) as  PRODUCT_CODE_DIFF ,
    sum(P_TYPE_DIFF) as  P_TYPE_DIFF 
    from REG_DATA a
    where 1=1
    and re_id_qa = 100
    and re_id_pd = 38
    group by company, legacy_company_code_qa 

COMPANY	COMPANY_CODE	FEED_TYPE_DIFF	DATA_SOURCE_DIFF	ISSUE_DATE_DIFF	SEX_PRIMARY_DIFF	TAX_STATUS_DIFF	PRODUCT_NAME_DIFF	PRODUCT_CODE_DIFF	P_TYPE_DIFF
USAL	FASA	0	0	0	0	0	0	0	0
USAL	USAL	0	0	0	0	0	0	0	0
USAL	IAM	0	0	0	0	0	0	0	0


with diff_data as 
  (select company as company, company_code as company_code, 
    sum(FEED_TYPE_DIFF) as FEED_TYPE_DIFF, 
    sum(DATA_SOURCE_DIFF) as  DATA_SOURCE_DIFF, 
    sum(ISSUE_DATE_DIFF) as ISSUE_DATE_DIFF, 
    sum(SEX_PRIMARY_DIFF) as SEX_PRIMARY_DIFF, 
    sum(TAX_STATUS_DIFF) as  TAX_STATUS_DIFF ,
    sum(PRODUCT_NAME_DIFF) as PRODUCT_NAME_DIFF ,
    sum(PRODUCT_CODE_DIFF) as  PRODUCT_CODE_DIFF ,
    sum(P_TYPE_DIFF) as  P_TYPE_DIFF 
    from REG_DATA a
    where 1=1
    and re_id_qa = 100
    and re_id_pd = 38
    group by company, company_code )
select company, company_code, Diff_column ,  Diff_value  from diff_data
unpivot (Diff_value for Diff_column IN 
(FEED_TYPE_DIFF, DATA_SOURCE_DIFF, ISSUE_DATE_DIFF, SEX_PRIMARY_DIFF , 
TAX_STATUS_DIFF , PRODUCT_NAME_DIFF , PRODUCT_CODE_DIFF , P_TYPE_DIFF));

COMPANY	COMPANY_CODE	DIFF_COLUMN	DIFF_VALUE
USAL	FASA	FEED_TYPE_DIFF		0
USAL	FASA	DATA_SOURCE_DIFF	0
USAL	FASA	ISSUE_DATE_DIFF		0
USAL	FASA	SEX_PRIMARY_DIFF	0
USAL	FASA	TAX_STATUS_DIFF		0
USAL	FASA	PRODUCT_NAME_DIFF	0
USAL	FASA	PRODUCT_CODE_DIFF	0
USAL	FASA	POLICY_TYPE_DIFF	0
USAL	USAL	FEED_TYPE_DIFF		0
USAL	USAL	DATA_SOURCE_DIFF	0
USAL	USAL	ISSUE_DATE_DIFF		0
USAL	USAL	SEX_PRIMARY_DIFF	0
USAL	USAL	TAX_STATUS_DIFF		0
USAL	USAL	PRODUCT_NAME_DIFF	0
USAL	USAL	PRODUCT_CODE_DIFF	0
USAL	USAL	POLICY_TYPE_DIFF	0
USAL	IAM	FEED_TYPE_DIFF		0
USAL	IAM	DATA_SOURCE_DIFF	0
USAL	IAM	ISSUE_DATE_DIFF		0
USAL	IAM	SEX_PRIMARY_DIFF	0
USAL	IAM	TAX_STATUS_DIFF		0
USAL	IAM	PRODUCT_NAME_DIFF	0
USAL	IAM	PRODUCT_CODE_DIFF	0
USAL	IAM	POLICY_TYPE_DIFF	0

-================================================
-' Create dynamic query using listagg
-================================================

--with diff_data as 
--  (select company as company, company_code as company_code, 
-'    /###### sum_list ###########/
--    sum(FEED_TYPE_DIFF) as FEED_TYPE_DIFF, 
--    sum(DATA_SOURCE_DIFF) as  DATA_SOURCE_DIFF, 
--    sum(ISSUE_DATE_DIFF) as ISSUE_DATE_DIFF, 
--    sum(SEX_PRIMARY_DIFF) as SEX_PRIMARY_DIFF, 
--    sum(TAX_STATUS_DIFF) as  TAX_STATUS_DIFF ,
--    sum(PRODUCT_NAME_DIFF) as PRODUCT_NAME_DIFF ,
--    sum(PRODUCT_CODE_DIFF) as  PRODUCT_CODE_DIFF ,
--    sum(P_TYPE_DIFF) as  P_TYPE_DIFF 
-'    /###### sum_list end ###########/
--    from REG_DATA a
--    where 1=1
--    and re_id_qa = 100
--    and re_id_pd = 38
--    group by company, company_code )
--select company, company_code, Diff_column ,  Diff_value  from diff_data
--unpivot (Diff_value for Diff_column IN (
-' /###### col_list ###########/
--FEED_TYPE_DIFF, DATA_SOURCE_DIFF, ISSUE_DATE_DIFF, SEX_PRIMARY_DIFF , 
--TAX_STATUS_DIFF , PRODUCT_NAME_DIFF , PRODUCT_CODE_DIFF , P_TYPE_DIFF
-' /###### col_list end ###########/
--));



    --listagg(' sum('||column_name||') as '||column_name,',') within group (order by column_id) as sum_list, -- ora ORA-01489: workaround
    select rtrim(xmlagg(xmlelement(e,' sum('||column_name||') as '||column_name,',').extract('//text()') order by column_id).GetClobVal(),',') as sum_list,
           listagg(column_name,', ') within group (order by column_id) as col_list
           into lv_sum_str, lv_col_str
    from all_tab_columns 
    where table_name = 'REG_DATA' and column_name like '%DIFF' 
    order by column_id;

    lv_final_str := 'insert into reg_diff
      (company, company_code, diff_column, diff_value) 
      with diff_data as 
      (select company as company, company_code , '||
        lv_sum_str ||
       ' from REG_DATA a
         where 1=1
         and iteration_id_qa = '|| pn_iteration_id_test ||
       ' and iteration_id_pd = '|| pn_iteration_id_prod ||
         group by  company, company_code )
     select company, company_code, Diff_column ,  Diff_value  
     from diff_data
         unpivot (Diff_value for Diff_column IN ('|| lv_col_str ||'))'; 
         
    execute immediate lv_final_str ; 
    commit;