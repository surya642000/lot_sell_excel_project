import pandas as pd
import sys
import re
from datetime import datetime
import time

overall_process_start_time = time.time()

Sell_Letter_Path = 'Input/SL_Letter_Folder/SL_Letter.xlsx'
sell_letter = pd.read_excel(Sell_Letter_Path,skiprows=1)
Sell_Letter = pd.ExcelFile(Sell_Letter_Path).sheet_names

Sale_Dump_Path = 'Input/Sale_Dump_Folder/Sales_Dump.pkl'
Sale_Dump = pd.read_pickle(Sale_Dump_Path)
print(len(Sale_Dump))

Price_List_Path = 'Input/Price_List_Folder/PIM PRICELIST JUNE 2023.xlsb'
Price_List = pd.read_excel(Price_List_Path, sheet_name='PRICELIST BASE', engine='pyxlsb',skiprows=8)
test_output1 = "Program_Assets/Output/Test_data1.xlsx"
test_output = "Program_Assets/Output/Test_data.xlsx"
test_output2 = "Program_Assets/Output/Test_data2.xlsx"

sheet_data = {}

for sheet_name in Sell_Letter:
    df = pd.read_excel(Sell_Letter_Path, sheet_name=sheet_name,skiprows=1)
    df.columns = df.columns.str.strip()
    sheet_data[sheet_name] = df

Price_List['it_barcode'] = Price_List['Item Barcode'].apply('{:.0f}'.format)


for sheet_name, df_data in sheet_data.items():
    print("sheet_name", sheet_name)
    # print("df_data", df_data)
    matched_code_df = pd.DataFrame()
    unmatched_code_df = pd.DataFrame()
    concat_matched_spl_Data_df = pd.DataFrame()
    final_lot_sell_df = pd.DataFrame()
    final_lot_sell_dfs = pd.DataFrame()

    data_df = pd.DataFrame(df_data)

    append_matched_case_code_df = pd.DataFrame()

    for code_index, spl_code_data in data_df.iterrows():
        print("code_index", code_index)
        print("spl_code_data", spl_code_data)

        list_matched_data = pd.DataFrame()
        first_three_letters = pd.DataFrame()

        first_three_letters = spl_code_data['Special Code'][:3]
        
        matched_code_df = Sale_Dump[Sale_Dump['scheme_code'].str.replace(r'[\[\]]', '', regex=True)== spl_code_data['Special Code']]
        unmatched_code_df = Sale_Dump[Sale_Dump['scheme_code'].str.replace(r'[\[\]]', '', regex=True) != spl_code_data['Special Code']]        
        unmatched_code_df = unmatched_code_df.dropna(subset=['scheme_code'])
        unmatched_code_df = pd.DataFrame(unmatched_code_df)
        unmatched_code_df = unmatched_code_df.reset_index(drop=True)

        pattern = r'^\[.*\] \[.*\]$'
        matches = unmatched_code_df['scheme_code'].str.contains(pattern)

        contains_df = unmatched_code_df[matches]
        contains_df = contains_df.reset_index(drop=True)

        print("Matched data ",matched_code_df)

        matched_code_df = pd.concat([matched_code_df, contains_df], axis=0)
        match_mon = matched_code_df['scheme_code'].str.contains(first_three_letters)
        contains_match_mon = matched_code_df[match_mon]
        contains_match_mon = contains_match_mon.reset_index(drop=True)
        matched_code_df = pd.DataFrame(contains_match_mon)        
        print("After getting contain data : ",len(matched_code_df))
        # matched_code_df.to_excel("sample.xlsx",index = False)

        remove_duplicates = pd.DataFrame()
        remove_duplicates['case'] = matched_code_df['name'].drop_duplicates().reset_index(drop=True)

        list_matched_data = Price_List[Price_List['Item Barcode'] == spl_code_data['Base Barcode']]
        remove_duplicates['Case'] = remove_duplicates['case'].drop_duplicates().reset_index(drop=True)
        

        for ccrd_data in remove_duplicates['Case']:
            # print(ccrd_data)
            matched_case_code_df = pd.DataFrame()
            matched_case_code_range_df = pd.DataFrame()
            matched_case_df = pd.DataFrame()

            if 'DP Lotsell' in sheet_name:
                if '82293897J' != ccrd_data:
                    
                    matched_case_df = matched_code_df[matched_code_df['name'] == float(ccrd_data)]
                else:
                    
                    matched_case_df = matched_code_df[matched_code_df['name'] == ccrd_data]
            
            if 'Lotsell Plan' in sheet_name:
                def is_float(string):
                    try:
                        float(string)
                        return True
                    except ValueError:
                        return False
                    
                if is_float(ccrd_data):
                    matched_case_df = matched_code_df[matched_code_df['name'] == float(ccrd_data)]
                else:
                    matched_case_df = matched_code_df[matched_code_df['name'] == (ccrd_data)]
            # else:
                matched_case_df = matched_code_df[matched_code_df['name'] == (ccrd_data)]    
            
            
            if 'BSPI' in sheet_name or 'HFS IGP' in sheet_name:
                matched_case_df = matched_code_df[matched_code_df['name'] == (ccrd_data)]

            if  'JR&R' in sheet_name:
                matched_case_df = matched_code_df[matched_code_df['name'] == float(ccrd_data)]
                # print(matched_case_df)

            if isinstance(ccrd_data, str):
                matched_case_df = matched_code_df[matched_code_df['name'] == (ccrd_data)]
            else: 
                matched_case_df = matched_code_df[matched_code_df['name'] == float(ccrd_data)]
            # print(matched_case_df)
            
            matched_case_df = pd.DataFrame(matched_case_df)
            matched_case_df = matched_case_df.reset_index(drop=True)
            mat_case_code_SO_len = len(matched_case_df)
            matched_case_code_range_df = pd.DataFrame(index=range(mat_case_code_SO_len))
            # print('matched_case_code_range_df', matched_case_code_range_df)
            
            if 'Lotsell Plan' in sheet_name:
                def is_float(string):
                    try:
                        float(string)
                        return True
                    except ValueError:
                        return False
                    
                if is_float(ccrd_data):
                    remove_duplicates = list_matched_data[list_matched_data['Case Code'] == float(ccrd_data)]
                else:
                    remove_duplicates = list_matched_data[list_matched_data['Case Code'] == (ccrd_data)]

            if 'DP Lotsell' in sheet_name:
                if '82293897J' != ccrd_data:
                    # print("yes")
                    matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == float(ccrd_data)]
                else:
                    matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == ccrd_data]

        
            if isinstance(ccrd_data, str):
                matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == (ccrd_data)]
            else:
                matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == float(ccrd_data)]
            

            if  'BSPI' in sheet_name or 'HFS IGP' in sheet_name:
                matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == float(ccrd_data)]
                # print('matched_case_code_df', matched_case_code_df)
            if  'JR&R' in sheet_name:
                matched_case_code_df = list_matched_data[list_matched_data['Case Code'] == (ccrd_data)]
                # print('matched_case_code_df', matched_case_code_df)
            
            matched_case_code_df = pd.DataFrame(matched_case_code_df)

            if 'JR&R' in sheet_name or 'BSPI' in sheet_name:
                matched_case_code_range_df['Slab 1'] = spl_code_data['Discount']
                matched_case_df['Slab 1'] = matched_case_code_range_df['Slab 1']

                matched_case_code_range_df['Slab 2'] = spl_code_data['Discount.1']
                matched_case_df['Slab 2'] = matched_case_code_range_df['Slab 2']

                matched_case_code_range_df['Slab 3'] = spl_code_data['Discount.2']
                matched_case_df['Slab 3'] = matched_case_code_range_df['Slab 3']
            else:
                matched_case_code_range_df['Eligible Discount depth as per Sales letter'] = spl_code_data['Discount']
                matched_case_df['Eligible Discount depth as per Sales letter'] = matched_case_code_range_df['Eligible Discount depth as per Sales letter']

            if not matched_case_code_df.empty:
                matched_case_code_range_df['Pcs/Selling UOM'] = matched_case_code_df['Pcs/\nSelling UOM'].values[0]
                # print(matched_case_code_range_df['Pcs/Selling UOM'])
            else:
                matched_case_code_range_df['Pcs/Selling UOM'] = 0
                

            if not matched_case_code_df.empty:
                matched_case_code_range_df['LPTT/PC W/ VAT'] = matched_case_code_df['LPTT/PC\nW/ VAT'].values[0]
            else:
                matched_case_code_range_df['LPTT/PC W/ VAT'] = 0
                # print(matched_case_code_range_df['LPTT/PC W/ VAT'])

            matched_case_df['Pcs/Selling UOM'] = matched_case_code_range_df['Pcs/Selling UOM']
            
            matched_case_df['LPTT/PC W/ VAT'] = matched_case_code_range_df['LPTT/PC W/ VAT']

            
            if 'HFS IGP' in sheet_name:
                matched_case_dfsr = pd.DataFrame()
                column_cv = pd.DataFrame()
    
                if '4987176092267' == str(spl_code_data['Base Barcode']):

                    matched_case_df = matched_case_df[matched_case_df['distributor_site'] != 'JR&R Philippines Inc']
                    matched_case_df = matched_case_df.reset_index(drop=True)                
                    mask = (matched_case_df['distributor_site'] == 'Washington DC Distributors') & (matched_case_df['pg_local_subsegment'] == 'HFS Small')
                    matched_case_df = matched_case_df[~mask]

                    matched_case_df = matched_case_df.reset_index(drop=True) 
                    column_cv = pd.DataFrame(index=range(len(matched_case_df['pg_local_subsegment'])))
                    column_cv['Scheme applicable'] = 'yes'
                    matched_case_df['Scheme applicable'] = column_cv['Scheme applicable'] 
                    # print(len(matched_case_df))  
                
                elif '4902430635370' == str(spl_code_data['Base Barcode']):
                    matched_case_df = matched_case_df[matched_case_df['pg_local_subsegment'] != 'HFS Small']
                    matched_case_df = matched_case_df[matched_case_df['pg_local_subsegment'] != 'SubD']
                    matched_case_df = matched_case_df.reset_index(drop=True) 
                    column_cv = pd.DataFrame(index=range(len(matched_case_df['pg_local_subsegment'])))
                    column_cv['Scheme applicable'] = 'yes'
                    matched_case_df['Scheme applicable'] = column_cv['Scheme applicable'] 

                elif '4902430333597' == str(spl_code_data['Base Barcode']):
                    matched_case_df = matched_case_df[matched_case_df['pg_local_subsegment'] != 'HFS Small']
                    matched_case_df = matched_case_df[matched_case_df['pg_local_subsegment'] != 'HFS Med']
                    matched_case_df = matched_case_df[matched_case_df['pg_local_subsegment'] != 'SubD']
                    matched_case_df = matched_case_df.reset_index(drop=True) 
                    column_cv = pd.DataFrame(index=range(len(matched_case_df['pg_local_subsegment'])))
                    column_cv['Scheme applicable'] = 'yes'
                    matched_case_df['Scheme applicable'] = column_cv['Scheme applicable'] 
                
                elif '4902430764858' == str(spl_code_data['Base Barcode']):
                    column_cv = pd.DataFrame(index=range(len(matched_case_df['pg_local_subsegment'])))
                    column_cv['Scheme applicable'] = 'yes'
                    matched_case_df['Scheme applicable'] = column_cv['Scheme applicable']
                    
                    
                matched_case_df = matched_case_df.reset_index(drop=True) 
                    
            matched_case_df = matched_case_df.dropna(axis=1, how='all')

            append_matched_case_code_df = pd.concat([append_matched_case_code_df, matched_case_df])
            # print(append_matched_case_code_df.columns)
        
        print('append_matched_case_code_df', append_matched_case_code_df)
        # append_matched_case_code_df.to_excel(test_output,index=False)
            
    # append_matched_case_code_df.to_excel(test_output,index=False)

    group_matched_df = pd.DataFrame()

    if 'HFS IGP' in sheet_name :
        group_matched_df = append_matched_case_code_df.groupby(['distributor_site','branch', 'scheme_code','scheme_group_name','pg_local_subsegment', 'customer_id', 'it_barcode',  'Pcs/Selling UOM','LPTT/PC W/ VAT','Scheme applicable','Eligible Discount depth as per Sales letter'], sort=False, as_index=False).agg({"item_qty": "sum", "giv": "sum", "scheme_value": "sum"})

    elif 'JR&R' in sheet_name or 'BSPI' in sheet_name:
        group_matched_df = append_matched_case_code_df.groupby(['distributor_site','branch', 'scheme_code','scheme_group_name','pg_local_subsegment', 'customer_id', 'it_barcode',  'Pcs/Selling UOM','LPTT/PC W/ VAT','Slab 1','Slab 2','Slab 3'], sort=False, as_index=False).agg({"item_qty": "sum", "giv": "sum", "scheme_value": "sum"})
        
    else:
        group_matched_df = append_matched_case_code_df.groupby(['distributor_site','branch', 'scheme_code','scheme_group_name','pg_local_subsegment', 'customer_id', 'it_barcode',  'Pcs/Selling UOM','LPTT/PC W/ VAT','Eligible Discount depth as per Sales letter'], sort=False, as_index=False).agg({"item_qty": "sum", "giv": "sum", "scheme_value": "sum"})    
    print('group_matched_df', group_matched_df)

    final_lot_sell_df['Distributor'] = group_matched_df['distributor_site']
    final_lot_sell_df['Barcode'] = group_matched_df['it_barcode'] 
    final_lot_sell_df['Branch'] = group_matched_df['branch'] 
    final_lot_sell_df['Customer ID'] = group_matched_df['customer_id'] 
    final_lot_sell_df['scheme_code'] = group_matched_df['scheme_code'] 
    final_lot_sell_df['scheme_group_name'] = group_matched_df['scheme_group_name']
    final_lot_sell_df['pg_local_subsegment'] = group_matched_df['pg_local_subsegment'] 
    final_lot_sell_df['Sum of item quality'] = group_matched_df['item_qty'] 
    final_lot_sell_df['Sum of giv'] = group_matched_df['giv'] 
    final_lot_sell_df['scheme_value'] = group_matched_df['scheme_value'] 

    final_lot_sell_dfr = pd.DataFrame()

    if 'DP Lotsell' not in sheet_name and 'JR&R' not in sheet_name:
        final_lot_sell_dfr = final_lot_sell_df['scheme_group_name'].str.contains(first_three_letters)
        final_lot_sell_df = final_lot_sell_df[final_lot_sell_dfr]
        # print('final_lot_sell_df', final_lot_sell_df)

    elif 'DP Lotsell' in sheet_name:
        final_lot_sell_dfr = final_lot_sell_df['scheme_group_name'].str.contains('Jun')
        final_lot_sell_df = final_lot_sell_df[final_lot_sell_dfr]
        final_lot_sell_df = final_lot_sell_df.reset_index(drop=True) 
    # print("final_lot_sell_df11",final_lot_sell_df)

    if 'HFS IGP' in sheet_name:
        final_lot_sell_df['Scheme applicable'] =  group_matched_df['Scheme applicable'] 

    if 'JR&R' in sheet_name:
        # print(final_lot_sell_df)
        final_lot_sell_df = final_lot_sell_df[final_lot_sell_df['scheme_group_name'].str.replace(r'[\[\]]', '', regex=True) == 'JRR Q-Lotsell (June 2023)']
        final_lot_sell_df = final_lot_sell_df.reset_index(drop=True) 
        # print(final_lot_sell_df)
        jrr_column_cv = pd.DataFrame(index=range(len(final_lot_sell_df['scheme_group_name'])))
        jrr_column_cv['Scheme applicable'] = 'yes'
        final_lot_sell_df['Scheme applicable'] = jrr_column_cv['Scheme applicable']

    elif 'BSPI' in sheet_name:
        final_lot_sell_df = final_lot_sell_df[final_lot_sell_df['scheme_group_name'].str.replace(r'[\[\]]', '', regex=True) == 'BSPI Q-Lotsell (June 2023)']
        final_lot_sell_df = final_lot_sell_df.reset_index(drop=True) 
        bspi_column_cv = pd.DataFrame(index=range(len(final_lot_sell_df['scheme_group_name'])))
        bspi_column_cv['Scheme applicable'] = 'yes'
        final_lot_sell_df['Scheme applicable'] = bspi_column_cv['Scheme applicable']

    elif 'Lotsell Plan' in sheet_name:
        final_lot_sell_dfdr = pd.DataFrame()

        def extract_single_pair_values(text):
            match = re.search(r'\[([^[\]]+)\]', text)
            return match.group(1) if match else None
        
        final_lot_sell_dfdr['scheme_group_name'] = final_lot_sell_df['scheme_group_name'].apply(extract_single_pair_values)
        
        final_lot_sell_df = final_lot_sell_df[final_lot_sell_df['scheme_group_name'].str.replace(r'[\[\]]', '', regex=True) == final_lot_sell_dfdr['scheme_group_name']]
        final_lot_sell_df = final_lot_sell_df.reset_index(drop=True) 
        # print(final_lot_sell_df)

        lotplan_column_cv = pd.DataFrame(index=range(len(final_lot_sell_df['scheme_group_name'])))
        lotplan_column_cv['Scheme applicable'] = 'yes'
        final_lot_sell_df['Scheme applicable'] = lotplan_column_cv['Scheme applicable']

    elif 'DP Lotsell' in sheet_name:

        dplot_column_cv = pd.DataFrame(index=range(len(final_lot_sell_df['scheme_group_name'])))
        dplot_column_cv['Scheme applicable'] = 'yes'
        final_lot_sell_df['Scheme applicable'] = dplot_column_cv['Scheme applicable']
        # print("final_lot",final_lot_sell_df)


    final_lot_sell_dfs['Executed Depth'] = (group_matched_df['scheme_value'] / group_matched_df['giv']).round(4)  
    final_lot_sell_df['Executed Depth'] = final_lot_sell_dfs['Executed Depth'].apply(lambda x: f"{x*100:.2f}%")
    final_lot_sell_df['Pcs/Selling UOM'] = group_matched_df['Pcs/Selling UOM'] 
    final_lot_sell_dfs['Cases Sold'] = (group_matched_df['item_qty'] / final_lot_sell_df['Pcs/Selling UOM']) 
    final_lot_sell_df['Cases Sold'] = final_lot_sell_dfs['Cases Sold'].round(4)
    final_lot_sell_dfs['Price as per Scheme in Sales report'] = (group_matched_df['giv'] / group_matched_df['item_qty']) 
    final_lot_sell_df['Price as per Scheme in Sales report'] = final_lot_sell_dfs['Price as per Scheme in Sales report'].round(4)
    final_lot_sell_df['LPTT/PC W/ VAT'] = group_matched_df['LPTT/PC W/ VAT'] 
    final_lot_sell_df['Recommended LPTT Without VAT'] = (final_lot_sell_df['LPTT/PC W/ VAT'] / 1.12).round(4)  

    if 'JR&R' in sheet_name or 'BSPI' in sheet_name:
        final_lot_sell_df['Slab 1'] = group_matched_df['Slab 1']
        final_lot_sell_df['Slab 2'] = group_matched_df['Slab 2']
        final_lot_sell_df['Slab 3'] = group_matched_df['Slab 3']
        final_lot_sell_df['Eligible Discount depth as per Sales letter'] = group_matched_df[['Slab 1', 'Slab 2', 'Slab 3']].max(axis=1)

    if 'HFS IGP' in sheet_name or 'Lotsell Plan' in sheet_name or 'DP Lotsell' in sheet_name:
        final_lot_sell_df['Eligible Discount depth as per Sales letter'] = group_matched_df['Eligible Discount depth as per Sales letter'] 

    promo_col_name= f'Promo Calculation for {sheet_name}'
    approve_col_name= f'Approved Value for {sheet_name}'
    final_lot_sell_df[promo_col_name] = final_lot_sell_df['Eligible Discount depth as per Sales letter'] * final_lot_sell_df['Recommended LPTT Without VAT'] * final_lot_sell_df['Sum of item quality']
    final_lot_sell_df[approve_col_name] = final_lot_sell_df[[promo_col_name, 'scheme_value']].min(axis=1)
    final_lot_sell_df['Rejection'] = final_lot_sell_df[approve_col_name] - final_lot_sell_df['scheme_value']
    final_lot_sell_df['Remarks'] = ""  
    print(len(final_lot_sell_df))

    sum_groupby_final_lot_sell_df = final_lot_sell_df.groupby(['Branch'], sort=False, as_index=False).agg({'scheme_value': "sum", approve_col_name: "sum"})

    grand_total_app = final_lot_sell_df[approve_col_name].sum()
    grand_total_sch = final_lot_sell_df['scheme_value'].sum()

    grand_total_df = pd.concat([
        sum_groupby_final_lot_sell_df,
        pd.DataFrame({'Branch': ['Grand Total'],'scheme_value': [grand_total_sch],approve_col_name: [grand_total_app]})], ignore_index=True)

    sum_groupby_final_lot_sell_df = pd.DataFrame( grand_total_df)

    output_file = f'Output/{sheet_name}.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:

        sum_groupby_final_lot_sell_df.to_excel(writer, sheet_name='PIVOT',index=False, header=True)

        final_lot_sell_df.to_excel(writer, sheet_name=sheet_name,index=False, header=True)

print('Done')   

