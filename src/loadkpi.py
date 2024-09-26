# importing the module
import json
import pandas as pd
from omegaconf import OmegaConf
from numbers import Number

class LoadKPIJson:
    def __init__(self):
        # Opening JSON file
        with open(conf.file.jsonPath) as json_file:
            self.data = json.load(json_file)
        return

    def is_string(self, s):
        if isinstance(s, Number):
            return False
        else:
            return True

    def getKPIObject(self, typeObj="KPI"):
        listob = []
        for items in range(len(self.data)):
            if self.data[items].get('type') == typeObj:
                listob.append(self.data[items])
        return listob

    def createKPIDictionary(self, kpi_Obj):
        for kpi in kpi_Obj:
            kpi_dict = {}
            for jsonColumn, excelColumn in conf.get('kpi').items():
                if excelColumn:
                    li = []
                    for kpi_details in kpi.get('data'):
                        li.append(kpi_details.get(jsonColumn))
                    kpi_dict[excelColumn] = li
        self.kpiData = pd.DataFrame.from_dict(kpi_dict)
        self.kpiList = kpi_dict
        return

    def createfilterMapping(self, kpi_Obj):
        kpi_list = self.kpiList.get('ID')
        no_of_kpi = len(kpi_list)
        for kpi in kpi_Obj:
            kpi_dict = {}
            for filterid in ['kpiId', 'filterId','filterValue']:
                li = []
                for filters in kpi.get('filterMapping'):
                    li.append(filters.get(filterid))
                kpi_dict[filterid] = li
        filterMapping = pd.DataFrame.from_dict(kpi_dict)
        s = filterMapping['filterId'].value_counts()
        self.list_of_default_filters = list(s.loc[lambda x : x == no_of_kpi].index)
        self.list_of_kpi_specific_filters = list(s.loc[lambda x : x != no_of_kpi].index)
        self.kpi_filter_df = filterMapping.loc[filterMapping['filterId'].isin(self.list_of_kpi_specific_filters)]
        return

    def createDF(self, series):
        li=[]
        for items in series:
            if self.is_string(items):
                if type(items) == dict:
                    li.append(items)
                elif type(items[0]) == dict:
                    li.append(items[0])
                else:
                    li.append({'Unknown':None})
            else:
                li.append({'Unknown':None})
        return pd.DataFrame.from_dict(li)

    def getColumnName(self, runSql):
        return runSql.split(" ")[0]

    def createFilters(self):
        filterList = self.getKPIObject(typeObj="FILTER")
        default_filter_list = []
        filter_list = []
        default_and_kpi_list = self.list_of_kpi_specific_filters + self.list_of_default_filters
        for filters in filterList:
            if filters.get('id') in self.list_of_kpi_specific_filters:
                self.kpiData.loc[self.kpiData['ID'] == filters.get('id'), 'extraFilter'] = filters.get('runSql')
            if filters.get('id') in self.list_of_default_filters:
                default_filter_list.append(filters)
            if filters.get('id') not in default_and_kpi_list:
                filter_list.append(filters)
        self.default_filter = pd.DataFrame(default_filter_list)
        self.filters_df = pd.DataFrame.from_dict(filter_list)
        return self.filters_df

    def createAdvanceFilter(self, filter_df):
        filter_df.loc[:, 'runSql'] = filter_df.loc[:, 'runSql'].apply(self.getColumnName)
        samCondition = self.createDF(filter_df.loc[:, 'samDrill'])
        samMapping = self.createDF(samCondition.loc[:, 'mapping'])
        samDrill_df = pd.concat([samCondition, samMapping], axis=1, join='outer')
        samDrill_df.drop(['mapping', 'Unknown'], axis=1, inplace=True)
        samDrill_df.rename(columns=conf.filter.samDrill, inplace=True)
        filter_sample = filter_df.drop(['samDrill'], axis=1)
        filter_df_list = []
        for columns in conf.filter.keys():
            if columns in list(filter_sample.columns):
                filter_df_list.append(filter_sample.loc[:, columns])
        filter_df_list.append(samDrill_df)
        filter_df_final = pd.concat(filter_df_list, axis=1, join='outer')
        # map excel column to json
        colName = {key:values for key, values in conf.filter.items() if type(values) == str}
        filter_df_final.rename(columns=colName, inplace=True)
        self.filterDf = filter_df_final
        return
        
                

    def exportToXLSX(self):
        # creating an ExcelWriter object
        with pd.ExcelWriter(conf.file.excelPath) as writer:
            # writing to the 'Employee' sheet
            self.kpiData.to_excel(writer, sheet_name='KPI', index=False)
            self.default_filter.to_excel(writer, sheet_name='Default_Filters', index=False)
            self.filterDf.to_excel(writer, sheet_name='Advance_Filters', index=False)
        print('DataFrames are written to Excel File successfully.')
        return
    
if __name__ == "__main__":

    # ready the config file
    conf = OmegaConf.load('nam_config.yaml')

    # load the json for the dimention
    loadJSON = LoadKPIJson()

    # extract the KPI list from data object
    list_of_kpi = loadJSON.getKPIObject()

    loadJSON.createKPIDictionary(list_of_kpi)
                                
    loadJSON.createfilterMapping(list_of_kpi)

    defaultFilter = loadJSON.createFilters()

    loadJSON.createAdvanceFilter(defaultFilter)

    # convert the kpi list to dataframe
    # loadJSON.to_dataframe(list_of_kpi)

    # export the dataframe to excel
    loadJSON.exportToXLSX()




