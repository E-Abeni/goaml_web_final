from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType
from datetime import date, time
from typing import List, Optional, Dict, Any, TypeVar, Generic
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TransactionData:
    
    def __init__(self,
                 transactionId: str, branchName: Optional[str] = None,
                 transactionDate: Optional[str] = None, transactionTime: Optional[str] = None,
                 transactionType: Optional[str] = None, conductingManner: Optional[str] = None,
                 currencyType: Optional[str] = None, amountInBirr: Optional[str] = None,
                 amountInCurrency: Optional[str] = None, involverName: Optional[str] = None,
                 entityName: Optional[str] = None, otherName: Optional[str] = None,
                 sex: Optional[str] = None, birthDate: Optional[str] = None,
                 idCardNo: Optional[str] = None, passportNo: Optional[str] = None,
                 passportIssuedBy: Optional[str] = None, residenceCountry: Optional[str] = None,
                 originCountry: Optional[str] = None, occupation: Optional[str] = None,
                 country: Optional[str] = None, region: Optional[str] = None,
                 city: Optional[str] = None, subCity: Optional[str] = None,
                 woreda: Optional[str] = None, houseNo: Optional[str] = None,
                 postalCode: Optional[str] = None, businessMobileNo: Optional[str] = None,
                 bussinessTelNo: Optional[str] = None, businessFaxNo: Optional[str] = None,
                 residenceTelNo: Optional[str] = None, emailAddress: Optional[str] = None,
                 accountNo: Optional[str] = None, accHolderBranch: Optional[str] = None,
                 accOwnerName: Optional[str] = None, accountType: Optional[str] = None,
                 openedDate: Optional[str] = None, balanceHeld: Optional[str] = None,
                 balanceHeldDate: Optional[str] = None, closedDate: Optional[str] = None,
                 bussinessType: Optional[str] = None, registrationNo: Optional[str] = None,
                 taxIdNumber: Optional[str] = None, taxIdIssuedBy: Optional[str] = None,
                 benFullName: Optional[str] = None, benAccountNo: Optional[str] = None,
                 benBranchID: Optional[str] = None, benBranchName: Optional[str] = None,
                 benOwnerEntity: Optional[str] = None, benCountry: Optional[str] = None,
                 benRegion: Optional[str] = None, benCity: Optional[str] = None,
                 benZone: Optional[str] = None, benWoreda: Optional[str] = None,
                 benHouseNo: Optional[str] = None, benTelNo: Optional[str] = None,
                 benIsEntity: Optional[str] = None, entity_Name: Optional[str] = None,
                 typeOfConducting: Optional[str] = None, reportDate: Optional[str] = None,
                 reportStatus: Optional[str] = None, reason: Optional[str] = None,
                 fullName: Optional[str] = None): 

        self.transactionId = transactionId
        self.branchName = branchName
        self.transactionDate = transactionDate
        self.transactionTime = transactionTime
        self.transactionType = transactionType
        self.conductingManner = conductingManner 
        self.currencyType = currencyType
        self.amountInBirr = amountInBirr
        self.amountInCurrency = amountInCurrency
        self.involverName = involverName if involverName is not None else fullName
        self.entityName = entityName
        self.otherName = otherName
        self.sex = sex
        self.birthDate = birthDate
        self.idCardNo = idCardNo
        self.passportNo = passportNo
        self.passportIssuedBy = passportIssuedBy
        self.residenceCountry = residenceCountry
        self.originCountry = originCountry
        self.occupation = occupation
        self.country = country
        self.region = region
        self.city = city
        self.subCity = subCity
        self.woreda = woreda
        self.houseNo = houseNo
        self.postalCode = postalCode
        self.businessMobileNo = businessMobileNo
        self.bussinessTelNo = bussinessTelNo
        self.businessFaxNo = businessFaxNo
        self.residenceTelNo = residenceTelNo
        self.emailAddress = emailAddress
        self.accountNo = accountNo
        self.accHolderBranch = accHolderBranch
        self.accOwnerName = accOwnerName
        self.accountType = accountType
        self.openedDate = openedDate
        self.balanceHeld = balanceHeld
        self.balanceHeldDate = balanceHeldDate
        self.closedDate = closedDate
        self.bussinessType = bussinessType
        self.registrationNo = registrationNo
        self.taxIdNumber = taxIdNumber
        self.taxIdIssuedBy = taxIdIssuedBy
        self.benFullName = benFullName
        self.benAccountNo = benAccountNo
        self.benBranchID = benBranchID
        self.benBranchName = benBranchName
        self.benOwnerEntity = benOwnerEntity
        self.benCountry = benCountry
        self.benRegion = benRegion
        self.benCity = benCity
        self.benZone = benZone
        self.benWoreda = benWoreda
        self.benHouseNo = benHouseNo
        self.benTelNo = benTelNo
        self.benIsEntity = benIsEntity
        self.entity_Name = entity_Name
        self.typeOfConducting = typeOfConducting 
        self.reportDate = reportDate
        self.reportStatus = reportStatus
        self.reason = reason
        self.fullName = fullName


    def to_dict(self) -> Dict[str, Any]:
       
        data_dict = {}
       
        attributes = [
            'transactionId', 'branchName', 'transactionDate', 'transactionTime',
            'transactionType', 'conductingManner', 'currencyType', 'amountInBirr',
            'amountInCurrency', 'involverName', 'entityName', 'otherName', 'sex',
            'birthDate', 'idCardNo', 'passportNo', 'passportIssuedBy',
            'residenceCountry', 'originCountry', 'occupation', 'country', 'region',
            'city', 'subCity', 'woreda', 'houseNo', 'postalCode', 'businessMobileNo',
            'bussinessTelNo', 'businessFaxNo', 'residenceTelNo', 'emailAddress',
            'accountNo', 'accHolderBranch', 'accOwnerName', 'accountType',
            'openedDate', 'balanceHeld', 'balanceHeldDate', 'closedDate',
            'bussinessType', 'registrationNo', 'taxIdNumber', 'taxIdIssuedBy',
            'benFullName', 'benAccountNo', 'benBranchID', 'benBranchName',
            'benOwnerEntity', 'benCountry', 'benRegion', 'benCity', 'benZone',
            'benWoreda', 'benHouseNo', 'benTelNo', 'benIsEntity', 'entity_Name',
            'typeOfConducting', 'reportDate', 'reportStatus', 'reason'
             'fullName'
        ]

        for attr in attributes:
            value = getattr(self, attr, None)
            if value is None:
                data_dict[attr] = None
            elif isinstance(value, (date, time)):
                data_dict[attr] = value.isoformat()
            elif isinstance(value, bool):
                data_dict[attr] = '1' if value else '0' 
            else:
                data_dict[attr] = str(value) 
        return data_dict

    @staticmethod
    def from_spark_row(row) -> Optional['TransactionData']:
       
        if not row:
            return None
        return TransactionData(
            transactionId=getattr(row, 'transactionid', None),
            branchName=getattr(row, 'branchname', None),
            transactionDate=getattr(row, 'transactiondate', None),
            transactionTime=getattr(row, 'transactiontime', None),
            transactionType=getattr(row, 'transactiontype', None),
            conductingManner=getattr(row, 'conductingmanner', None), 
            currencyType=getattr(row, 'currencytype', None),
            amountInBirr=getattr(row, 'amountinbirr', None),
            amountInCurrency=getattr(row, 'amountincurrency', None),
            involverName=getattr(row, 'involvername', None), 
            entityName=getattr(row, 'entityname', None), 
            otherName=getattr(row, 'othername', None),
            sex=getattr(row, 'sex', None),
            birthDate=getattr(row, 'birthdate', None),
            idCardNo=getattr(row, 'idcardno', None),
            passportNo=getattr(row, 'passportno', None),
            passportIssuedBy=getattr(row, 'passportissuedby', None),
            residenceCountry=getattr(row, 'residencecountry', None),
            originCountry=getattr(row, 'origincountry', None),
            occupation=getattr(row, 'occupation', None),
            country=getattr(row, 'country', None),
            region=getattr(row, 'region', None),
            city=getattr(row, 'city', None),
            subCity=getattr(row, 'subcity', None), 
            woreda=getattr(row, 'woreda', None),
            houseNo=getattr(row, 'houseno', None),
            postalCode=getattr(row, 'postalcode', None),
            businessMobileNo=getattr(row, 'businessmobileno', None),
            bussinessTelNo=getattr(row, 'bussinesstelno', None),
            businessFaxNo=getattr(row, 'businessfaxno', None),
            residenceTelNo=getattr(row, 'residencetelno', None),
            emailAddress=getattr(row, 'emailaddress', None),
            accountNo=getattr(row, 'accountno', None),
            accHolderBranch=getattr(row, 'accholderbranch', None),
            accOwnerName=getattr(row, 'accownername', None),
            accountType=getattr(row, 'accounttype', None),
            openedDate=getattr(row, 'openeddate', None),
            balanceHeld=getattr(row, 'balanceheld', None),
            balanceHeldDate=getattr(row, 'balancehelddate', None),
            closedDate=getattr(row, 'closeddate', None),
            bussinessType=getattr(row, 'bussinesstype', None), 
            registrationNo=getattr(row, 'registrationno', None), 
            taxIdNumber=getattr(row, 'taxidnumber', None), 
            taxIdIssuedBy=getattr(row, 'taxidissuedby', None), 
            benFullName=getattr(row, 'benfullname', None),
            benAccountNo=getattr(row, 'benaccountno', None),
            benBranchID=getattr(row, 'benbranchid', None), 
            benBranchName=getattr(row, 'benbranchname', None),
            benOwnerEntity=getattr(row, 'benownerentity', None),
            benCountry=getattr(row, 'bencountry', None),
            benRegion=getattr(row, 'benregion', None),
            benCity=getattr(row, 'bencity', None),
            benZone=getattr(row, 'benzone', None),
            benWoreda=getattr(row, 'benworeda', None),
            benHouseNo=getattr(row, 'benhouseno', None),
            benTelNo=getattr(row, 'bentelno', None),
            benIsEntity=getattr(row, 'benisentity', None),
            entity_Name=getattr(row, 'entity_name', None), 
            typeOfConducting=getattr(row, 'typeofconducting', None), 
            reportDate=getattr(row, 'reportdate', None),
            reportStatus=getattr(row, 'reportstatus', None), 
            reason=getattr(row, 'reason', None), 
            fullName=getattr(row, 'fullname', None) 
        )


class TransactionSearchRequest:
    
    def __init__(self,
                 transactionId: Optional[str] = None,
                 branchName: Optional[str] = None,
                 transactionDateStart: Optional[str] = None, transactionDateEnd: Optional[str] = None,
                 transactionTime: Optional[str] = None,
                 transactionType: Optional[str] = None, conductingManner: Optional[str] = None, 
                 currencyType: Optional[str] = None, minAmountInBirr: Optional[str] = None,
                 maxAmountInBirr: Optional[str] = None, minAmountInCurrency: Optional[str] = None,
                 maxAmountInCurrency: Optional[str] = None, involverName: Optional[str] = None, 
                 entityName: Optional[str] = None, 
                 otherName: Optional[str] = None, sex: Optional[str] = None,
                 birthDateStart: Optional[str] = None, birthDateEnd: Optional[str] = None,
                 idCardNo: Optional[str] = None, passportNo: Optional[str] = None,
                 passportIssuedBy: Optional[str] = None, residenceCountry: Optional[str] = None,
                 originCountry: Optional[str] = None, occupation: Optional[str] = None,
                 country: Optional[str] = None, region: Optional[str] = None,
                 city: Optional[str] = None, subCity: Optional[str] = None,
                 woreda: Optional[str] = None, houseNo: Optional[str] = None,
                 postalCode: Optional[str] = None, businessMobileNo: Optional[str] = None,
                 bussinessTelNo: Optional[str] = None, businessFaxNo: Optional[str] = None,
                 residenceTelNo: Optional[str] = None, emailAddress: Optional[str] = None,
                 accountNo: Optional[str] = None, accHolderBranch: Optional[str] = None,
                 accOwnerName: Optional[str] = None, accountType: Optional[str] = None,
                 openedDateStart: Optional[str] = None, openedDateEnd: Optional[str] = None,
                 minBalanceHeld: Optional[str] = None, maxBalanceHeld: Optional[str] = None,
                 balanceHeldDateStart: Optional[str] = None, balanceHeldDateEnd: Optional[str] = None,
                 closedDateStart: Optional[str] = None, closedDateEnd: Optional[str] = None,
                 bussinessType: Optional[str] = None, 
                 registrationNo: Optional[str] = None, 
                 taxIdNumber: Optional[str] = None, 
                 taxIdIssuedBy: Optional[str] = None, 
                 benFullName: Optional[str] = None, benAccountNo: Optional[str] = None,
                 benBranchID: Optional[str] = None, benBranchName: Optional[str] = None,
                 benOwnerEntity: Optional[str] = None, benCountry: Optional[str] = None,
                 benRegion: Optional[str] = None, benCity: Optional[str] = None,
                 benZone: Optional[str] = None, benWoreda: Optional[str] = None,
                 benHouseNo: Optional[str] = None, benTelNo: Optional[str] = None,
                 benIsEntity: Optional[str] = None,
                 entity_Name: Optional[str] = None, 
                 typeOfConducting: Optional[str] = None, 
                 reportDateStart: Optional[str] = None, reportDateEnd: Optional[str] = None,
                 reportStatus: Optional[str] = None, 
                 reason: Optional[str] = None, 
                 fullName: Optional[str] = None, 
                 page: Optional[int] = 0, size: Optional[int] = 10,
                 requireTotalSize: Optional[str] = None):

        self.transactionId = transactionId
        self.branchName = branchName
        self.transactionDateStart = transactionDateStart
        self.transactionDateEnd = transactionDateEnd
        self.transactionTime = transactionTime
        self.transactionType = transactionType
        self.conductingManner = conductingManner 
        self.currencyType = currencyType
        self.minAmountInBirr = minAmountInBirr
        self.maxAmountInBirr = maxAmountInBirr
        self.minAmountInCurrency = minAmountInCurrency
        self.maxAmountInCurrency = maxAmountInCurrency
        self.involverName = involverName if involverName is not None else fullName 
        self.entityName = entityName
        self.otherName = otherName
        self.sex = sex
        self.birthDateStart = birthDateStart
        self.birthDateEnd = birthDateEnd
        self.idCardNo = idCardNo
        self.passportNo = passportNo
        self.passportIssuedBy = passportIssuedBy
        self.residenceCountry = residenceCountry
        self.originCountry = originCountry
        self.occupation = occupation
        self.country = country
        self.region = region
        self.city = city
        self.subCity = subCity
        self.woreda = woreda
        self.houseNo = houseNo
        self.postalCode = postalCode
        self.businessMobileNo = businessMobileNo
        self.bussinessTelNo = bussinessTelNo
        self.businessFaxNo = businessFaxNo
        self.residenceTelNo = residenceTelNo
        self.emailAddress = emailAddress
        self.accountNo = accountNo
        self.accHolderBranch = accHolderBranch
        self.accOwnerName = accOwnerName
        self.accountType = accountType
        self.openedDateStart = openedDateStart
        self.openedDateEnd = openedDateEnd
        self.minBalanceHeld = minBalanceHeld
        self.maxBalanceHeld = maxBalanceHeld
        self.balanceHeldDateStart = balanceHeldDateStart
        self.balanceHeldDateEnd = balanceHeldDateEnd
        self.closedDateStart = closedDateStart
        self.closedDateEnd = closedDateEnd
        self.bussinessType = bussinessType
        self.registrationNo = registrationNo
        self.taxIdNumber = taxIdNumber
        self.taxIdIssuedBy = taxIdIssuedBy
        self.benFullName = benFullName
        self.benAccountNo = benAccountNo
        self.benBranchID = benBranchID
        self.benBranchName = benBranchName
        self.benOwnerEntity = benOwnerEntity
        self.benCountry = benCountry
        self.benRegion = benRegion
        self.benCity = benCity
        self.benZone = benZone
        self.benWoreda = benWoreda
        self.benHouseNo = benHouseNo
        self.benTelNo = benTelNo
        self.benIsEntity = benIsEntity
        self.entity_Name = entity_Name
        self.typeOfConducting = typeOfConducting
        self.reportDateStart = reportDateStart
        self.reportDateEnd = reportDateEnd
        self.reportStatus = reportStatus
        self.reason = reason
        self.fullName = fullName 
        self.page = page
        self.size = size
        self.requireTotalSize = requireTotalSize



T = TypeVar('T')

class PagedResponse(Generic[T]):
    
    def __init__(self, content: List[T], page: int, size: int, total_elements: int):
        self.content = content
        self.page = page
        self.size = size
        self.total_elements = total_elements
        self.total_pages = (total_elements + size - 1) // size if size > 0 else 0
        self.is_first = page == 0
        self.is_last = (page + 1) * size >= total_elements if total_elements > 0 else True
        self.has_next = (page + 1) * size < total_elements
        self.has_previous = page > 0

    def to_dict(self):
        return {
            "content": [item.to_dict() if hasattr(item, 'to_dict') else item for item in self.content],
            "page": self.page,
            "size": self.size,
            "totalElements": self.total_elements,
            "totalPages": self.total_pages,
            "isFirst": self.is_first,
            "isLast": self.is_last,
            "hasNext": self.has_next,
            "hasPrevious": self.has_previous
        }



class TransactionRepository:
    
    TRANSACTIONS_TABLE_NAME = "default.transactions" 
    
    TRANSACTION_SCHEMA = StructType([
        StructField("transactionid", StringType(), False),
        StructField("branchname", StringType(), True),
        StructField("transactiondate", StringType(), True),
        StructField("transactiontime", StringType(), True),
        StructField("transactiontype", StringType(), True),
        StructField("conductingmanner", StringType(), True), 
        StructField("currencytype", StringType(), True),
        StructField("amountinbirr", StringType(), True),
        StructField("amountincurrency", StringType(), True),
        StructField("involvername", StringType(), True), 
        StructField("entityname", StringType(), True), 
        StructField("othername", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("idcardno", StringType(), True),
        StructField("passportno", StringType(), True),
        StructField("passportissuedby", StringType(), True),
        StructField("residencecountry", StringType(), True),
        StructField("origincountry", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("subcity", StringType(), True),
        StructField("woreda", StringType(), True),
        StructField("houseno", StringType(), True),
        StructField("postalcode", StringType(), True),
        StructField("businessmobileno", StringType(), True),
        StructField("bussinesstelno", StringType(), True), 
        StructField("businessfaxno", StringType(), True),
        StructField("residencetelno", StringType(), True),
        StructField("emailaddress", StringType(), True),
        StructField("accountno", StringType(), True),
        StructField("accholderbranch", StringType(), True),
        StructField("accownername", StringType(), True),
        StructField("accounttype", StringType(), True),
        StructField("openeddate", StringType(), True),
        StructField("balanceheld", StringType(), True),
        StructField("balancehelddate", StringType(), True),
        StructField("closeddate", StringType(), True),
        StructField("bussinesstype", StringType(), True), 
        StructField("registrationno", StringType(), True), 
        StructField("taxidnumber", StringType(), True), 
        StructField("taxidissuedby", StringType(), True), 
        StructField("benfullname", StringType(), True),
        StructField("benaccountno", StringType(), True),
        StructField("benbranchid", StringType(), True), 
        StructField("benbranchname", StringType(), True),
        StructField("benownerentity", StringType(), True),
        StructField("bencountry", StringType(), True),
        StructField("benregion", StringType(), True),
        StructField("bencity", StringType(), True),
        StructField("benzone", StringType(), True),
        StructField("benworeda", StringType(), True),
        StructField("benhouseno", StringType(), True),
        StructField("bentelno", StringType(), True),
        StructField("benisentity", StringType(), True),
        StructField("entity_name", StringType(), True), 
        StructField("typeofconducting", StringType(), True),
        StructField("reportdate", StringType(), True),
        StructField("reportstatus", StringType(), True), 
        StructField("reason", StringType(), True), 
        StructField("fullname", StringType(), True) 
    ])


    def __init__(self, spark: SparkSession):
      
        self.spark = spark
        logger.info(f"TransactionRepository initialized for Hive table: {self.TRANSACTIONS_TABLE_NAME}")

    def _get_table_df(self):
       
        return self.spark.table(self.TRANSACTIONS_TABLE_NAME)

    def _build_search_conditions(self, search_request: TransactionSearchRequest):
       
        conditions = []

    
        column_mapping = {
            "transactionId": "transactionid",
            "branchName": "branchname",
            "transactionDateStart": "transactiondate",
            "transactionDateEnd": "transactiondate",
            "transactionTime": "transactiontime",
            "transactionType": "transactiontype",
            "conductingManner": "conductingmanner", 
            "currencyType": "currencytype",
            "minAmountInBirr": "amountinbirr",
            "maxAmountInBirr": "amountinbirr",
            "minAmountInCurrency": "amountincurrency",
            "maxAmountInCurrency": "amountincurrency",
            "involverName": "involvername", 
            "entityName": "entityname", 
            "otherName": "othername",
            "sex": "sex",
            "birthDateStart": "birthdate",
            "birthDateEnd": "birthdate",
            "idCardNo": "idcardno",
            "passportNo": "passportno",
            "passportIssuedBy": "passportissuedby",
            "residenceCountry": "residencecountry",
            "originCountry": "origincountry",
            "occupation": "occupation",
            "country": "country",
            "region": "region",
            "city": "city",
            "subCity": "subcity",
            "woreda": "woreda",
            "houseNo": "houseno",
            "postalCode": "postalcode",
            "businessMobileNo": "businessmobileno",
            "bussinessTelNo": "bussinesstelno", 
            "businessFaxNo": "businessfaxno",
            "residenceTelNo": "residencetelno",
            "emailAddress": "emailaddress",
            "accountNo": "accountno",
            "accHolderBranch": "accholderbranch",
            "accOwnerName": "accownername",
            "accountType": "accounttype",
            "openedDateStart": "openeddate",
            "openedDateEnd": "openeddate",
            "minBalanceHeld": "balanceheld",
            "maxBalanceHeld": "balanceheld",
            "balanceHeldDateStart": "balancehelddate",
            "balanceHeldDateEnd": "balancehelddate",
            "closedDateStart": "closeddate",
            "closedDateEnd": "closeddate",
            "bussinessType": "bussinesstype", 
            "registrationNo": "registrationno", 
            "taxIdNumber": "taxidnumber", 
            "taxIdIssuedBy": "taxidissuedby", 
            "benFullName": "benfullname",
            "benAccountNo": "benaccountno",
            "benBranchID": "benbranchid",
            "benBranchName": "benbranchname",
            "benOwnerEntity": "benownerentity",
            "benCountry": "bencountry",
            "benRegion": "benregion",
            "benCity": "bencity",
            "benZone": "benzone",
            "benWoreda": "benworeda",
            "benHouseNo": "benhouseno",
            "benTelNo": "bentelno",
            "benIsEntity": "benisentity",
            "entity_Name": "entity_name", 
            "typeOfConducting": "typeofconducting", 
            "reportDateStart": "reportdate",
            "reportDateEnd": "reportdate",
            "reportStatus": "reportstatus", 
            "reason": "reason", 
            "fullName": "fullname" 
        }

        def add_exact_match(attr_name: str, value: Optional[str]):
            column = column_mapping.get(attr_name)
            if column and value is not None and value.strip():
                conditions.append(f"{column} = '{value.strip()}'")

        def add_like_condition(attr_name: str, value: Optional[str]):
            column = column_mapping.get(attr_name)
            if column and value is not None and value.strip():
                conditions.append(f"{column} LIKE '%{value.strip()}%'")

        def add_range_condition(attr_name_min: str, attr_name_max: str, min_value: Optional[str], max_value: Optional[str]):
            column = column_mapping.get(attr_name_min) 
            if column:
                if min_value is not None and max_value is not None:
                    conditions.append(f"({column} >= '{min_value}' AND {column} <= '{max_value}')")
                elif min_value is not None:
                    conditions.append(f"{column} >= '{min_value}'")
                elif max_value is not None:
                    conditions.append(f"{column} <= '{max_value}'")

       
        add_exact_match("transactionId", search_request.transactionId)
        add_like_condition("branchName", search_request.branchName)
        add_exact_match("transactionTime", search_request.transactionTime)
        add_exact_match("transactionType", search_request.transactionType)
        add_exact_match("currencyType", search_request.currencyType)
        add_range_condition("minAmountInBirr", "maxAmountInBirr", search_request.minAmountInBirr, search_request.maxAmountInBirr)
        add_range_condition("minAmountInCurrency", "maxAmountInCurrency", search_request.minAmountInCurrency, search_request.maxAmountInCurrency)

        
        if search_request.involverName is not None and search_request.involverName.strip():
            add_like_condition("involverName", search_request.involverName)
        elif search_request.fullName is not None and search_request.fullName.strip():
            add_like_condition("fullName", search_request.fullName)

        add_like_condition("entityName", search_request.entityName) 
        add_like_condition("otherName", search_request.otherName)
        add_exact_match("sex", search_request.sex)
        add_exact_match("idCardNo", search_request.idCardNo)
        add_exact_match("passportNo", search_request.passportNo)
        add_exact_match("passportIssuedBy", search_request.passportIssuedBy)
        add_exact_match("residenceCountry", search_request.residenceCountry)
        add_exact_match("originCountry", search_request.originCountry)
        add_like_condition("occupation", search_request.occupation)

      
        add_exact_match("country", search_request.country)
        add_like_condition("region", search_request.region)
        add_like_condition("city", search_request.city)
        add_like_condition("subCity", search_request.subCity)
        add_like_condition("woreda", search_request.woreda)
        add_exact_match("houseNo", search_request.houseNo)
        add_exact_match("postalCode", search_request.postalCode)

        
        add_exact_match("businessMobileNo", search_request.businessMobileNo)
        add_exact_match("bussinessTelNo", search_request.bussinessTelNo)
        add_exact_match("businessFaxNo", search_request.businessFaxNo)
        add_exact_match("residenceTelNo", search_request.residenceTelNo)
        add_like_condition("emailAddress", search_request.emailAddress)

   
        add_exact_match("accountNo", search_request.accountNo)
        add_like_condition("accHolderBranch", search_request.accHolderBranch)
        add_like_condition("accOwnerName", search_request.accOwnerName)
        add_exact_match("accountType", search_request.accountType)
        add_range_condition("minBalanceHeld", "maxBalanceHeld", search_request.minBalanceHeld, search_request.maxBalanceHeld)

    
        add_like_condition("bussinessType", search_request.bussinessType)
        add_exact_match("registrationNo", search_request.registrationNo)
        add_exact_match("taxIdNumber", search_request.taxIdNumber)
        add_exact_match("taxIdIssuedBy", search_request.taxIdIssuedBy)

        
        add_like_condition("benFullName", search_request.benFullName)
        add_exact_match("benAccountNo", search_request.benAccountNo)
        add_exact_match("benBranchID", search_request.benBranchID)
        add_like_condition("benBranchName", search_request.benBranchName)
        add_like_condition("benOwnerEntity", search_request.benOwnerEntity)
        add_exact_match("benCountry", search_request.benCountry)
        add_like_condition("benRegion", search_request.benRegion)
        add_like_condition("benCity", search_request.benCity)
        add_like_condition("benZone", search_request.benZone)
        add_like_condition("benWoreda", search_request.benWoreda)
        add_exact_match("benHouseNo", search_request.benHouseNo)
        add_exact_match("benTelNo", search_request.benTelNo)

        
        if search_request.benIsEntity is not None:
            conditions.append(f"{column_mapping['benIsEntity']} = '{search_request.benIsEntity}'")

       
        add_like_condition("entity_Name", search_request.entity_Name)
        
        if search_request.typeOfConducting is not None and search_request.typeOfConducting.strip():
            add_exact_match("typeOfConducting", search_request.typeOfConducting)
        elif search_request.conductingManner is not None and search_request.conductingManner.strip():
            
            conditions.append(f"{column_mapping['typeOfConducting']} = '{search_request.conductingManner.strip()}'")

        add_exact_match("reportStatus", search_request.reportStatus)
        add_like_condition("reason", search_request.reason)


        
        add_range_condition("reportDateStart", "reportDateEnd", search_request.reportDateStart, search_request.reportDateEnd)
        add_range_condition("transactionDateStart", "transactionDateEnd", search_request.transactionDateStart, search_request.transactionDateEnd)
        add_range_condition("birthDateStart", "birthDateEnd", search_request.birthDateStart, search_request.birthDateEnd)
        add_range_condition("openedDateStart", "openedDateEnd", search_request.openedDateStart, search_request.openedDateEnd)
        add_range_condition("balanceHeldDateStart", "balanceHeldDateEnd", search_request.balanceHeldDateStart, search_request.balanceHeldDateEnd)
        add_range_condition("closedDateStart", "closedDateEnd", search_request.closedDateStart, search_request.closedDateEnd)

        return conditions

    def searchTransactions(self, search_request: TransactionSearchRequest) -> List[TransactionData]:
       
        logger.info(f"Executing searchTransactions with request: {search_request.__dict__}")
        try:
            sql_builder = [f"SELECT * FROM {self.TRANSACTIONS_TABLE_NAME}"]
            conditions = self._build_search_conditions(search_request)

            if conditions:
                sql_builder.append("WHERE " + " AND ".join(conditions))

           
            if search_request.size is not None and search_request.size > 0:
                offset = search_request.page * search_request.size if search_request.page is not None else 0
                sql_builder.append(f"LIMIT {search_request.size} OFFSET {offset}")

            final_sql = " ".join(sql_builder)
            logger.info(f"Executing PAGINATED SEARCH SQL: {final_sql}")

            df = self.spark.sql(final_sql)
            return [TransactionData.from_spark_row(row) for row in df.collect()]
        except Exception as e:
            logger.error(f"Error searching transactions: {e}", exc_info=True)
            return []

    def countTransactions(self, search_request: TransactionSearchRequest) -> int:
        
        logger.info(f"Executing countTransactions with request: {search_request.__dict__}")
        try:
            sql_builder = [f"SELECT COUNT(*) FROM {self.TRANSACTIONS_TABLE_NAME}"]
            conditions = self._build_search_conditions(search_request)

            if conditions:
                sql_builder.append("WHERE " + " AND ".join(conditions))

            count_sql = " ".join(sql_builder)
            logger.info(f"Executing COUNT SQL: {count_sql}")

            count_result = self.spark.sql(count_sql).collect()[0][0]
            logger.info(f"Counted {count_result} matching transactions.")
            return count_result
        except Exception as e:
            logger.error(f"Error counting transactions: {e}", exc_info=True)
            return 0



class TransactionService:
    
    def __init__(self, transaction_repository: TransactionRepository):
        
        self.transaction_repository = transaction_repository
        logger.info("TransactionService initialized.")

    def search_transactions(self, search_request: TransactionSearchRequest) -> PagedResponse[TransactionData]:
      
        page = search_request.page if search_request.page is not None and search_request.page >= 0 else 0
        size = search_request.size if search_request.size is not None and search_request.size > 0 else 20 

        search_request.page = page
        search_request.size = size

        total_elements = 0
       
        if hasattr(search_request, 'requireTotalSize') and search_request.requireTotalSize == '1':
            logger.info("Search Request with Total Size required.")

            total_elements = self.transaction_repository.countTransactions(search_request)
        else:
            logger.info("Search Request without Total Size required.")

        
        content = self.transaction_repository.searchTransactions(search_request)

        return PagedResponse(content, page, size, total_elements)



app = Flask(__name__)
CORS(app) 

spark_session: Optional[SparkSession] = None
transaction_repository: Optional[TransactionRepository] = None
transaction_service: Optional[TransactionService] = None

@app.before_request
def initialize_spark_and_services():
    
    global spark_session, transaction_repository, transaction_service
    if spark_session is None:
        logger.info("Initializing SparkSession and services...")
        spark_session = SparkSession.builder \
            .appName("TransactionHiveSearchAPI") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("hive.enforce.bucketing", "true") \
            .config("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager") \
            .config("hive.compactor.initiator.on", "true") \
            .config("hive.compactor.worker.threads", "1") \
            .enableHiveSupport() \
            .getOrCreate()
        
        transaction_repository = TransactionRepository(spark_session)
        transaction_service = TransactionService(transaction_repository)

      
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {transaction_repository.TRANSACTIONS_TABLE_NAME} (
            transactionid STRING,
            branchname STRING,
            transactiondate STRING,
            transactiontime STRING,
            transactiontype STRING,
            conductingmanner STRING,
            currencytype STRING,
            amountinbirr STRING,
            amountincurrency STRING,
            involvername STRING,
            entityname STRING,
            othername STRING,
            sex STRING,
            birthdate STRING,
            idcardno STRING,
            passportno STRING,
            passportissuedby STRING,
            residencecountry STRING,
            origincountry STRING,
            occupation STRING,
            country STRING,
            region STRING,
            city STRING,
            subcity STRING,
            woreda STRING,
            houseno STRING,
            postalcode STRING,
            businessmobileno STRING,
            bussinesstelno STRING,
            businessfaxno STRING,
            residencetelno STRING,
            emailaddress STRING,
            accountno STRING,
            accholderbranch STRING,
            accownername STRING,
            accounttype STRING,
            openeddate STRING,
            balanceheld STRING,
            balancehelddate STRING,
            closeddate STRING,
            bussinesstype STRING,
            registrationno STRING,
            taxidnumber STRING,
            taxidissuedby STRING,
            benfullname STRING,
            benaccountno STRING,
            benbranchid STRING,
            benbranchname STRING,
            benownerentity STRING,
            bencountry STRING,
            benregion STRING,
            bencity STRING,
            benzone STRING,
            benworeda STRING,
            benhouseno STRING,
            bentelno STRING,
            benisentity STRING,
            entity_name STRING,
            typeofconducting STRING,
            reportdate STRING,
            reportstatus STRING,
            reason STRING,
            fullname STRING
        )
        STORED AS ORC
        TBLPROPERTIES ('transactional'='true');
        """
        logger.info(f"Ensuring Hive table exists: {transaction_repository.TRANSACTIONS_TABLE_NAME}")
        spark_session.sql(create_table_sql)
        logger.info("Hive table setup complete.")

     
        current_count = transaction_repository.countTransactions(TransactionSearchRequest())
        if current_count == 0:
            logger.warning("No data found in the table. Inserting sample data for demonstration purposes.")
            sample_data = [
                TransactionData(transactionId="TX001", reportDate="2023-01-01", involverName="Alice Smith", amountInBirr="100.0", benIsEntity="0", typeOfConducting="Online"),
                TransactionData(transactionId="TX002", reportDate="2023-01-05", involverName="Bob Johnson", amountInBirr="200.0", benIsEntity="1", typeOfConducting="Branch"),
                TransactionData(transactionId="TX003", reportDate="2023-01-10", involverName="Charlie Brown", amountInBirr="150.0", benIsEntity="0", typeOfConducting="Mobile"),
                TransactionData(transactionId="TX004", reportDate="2023-01-15", involverName="David Lee", amountInBirr="300.0", benIsEntity="1", typeOfConducting="Online", entityName="Tech Corp"),
                TransactionData(transactionId="TX005", reportDate="2023-01-20", involverName="Eve Davis", amountInBirr="250.0", benIsEntity="0", typeOfConducting="Branch"),
                TransactionData(transactionId="TX006", reportDate="2023-01-25", involverName="Frank White", amountInBirr="180.0", benIsEntity="1", typeOfConducting="Mobile", benFullName="Grace Green")
            ]
          
            temp_data_for_df = []
            for tx in sample_data:
                tx_dict = tx.to_dict()
              
                lower_case_tx_dict = {k.lower(): v for k, v in tx_dict.items()}
            
                if 'conductingmanner' in lower_case_tx_dict:
                    lower_case_tx_dict['typeofconducting'] = lower_case_tx_dict.pop('conductingmanner')
                if 'involvername' in lower_case_tx_dict:
                    lower_case_tx_dict['fullname'] = lower_case_tx_dict['involvername']
                
                temp_data_for_df.append(lower_case_tx_dict)

            temp_df = spark_session.createDataFrame(temp_data_for_df, schema=transaction_repository.TRANSACTION_SCHEMA)
            temp_df.write.mode("append").insertInto(transaction_repository.TRANSACTIONS_TABLE_NAME)
            logger.warning(f"{len(sample_data)} sample transactions inserted for demonstration.")
        else:
            logger.info(f"Table already contains {current_count} records. Using existing data for search tests.")


@app.route('/api/transactions/search', methods=['POST'])
def search_transactions_api():
    
    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({"error": "Invalid JSON or empty request body"}), 400

      
        search_request = TransactionSearchRequest(
            transactionId=request_data.get('transactionId'),
            branchName=request_data.get('branchName'),
            transactionDateStart=request_data.get('transactionDateStart'),
            transactionDateEnd=request_data.get('transactionDateEnd'),
            transactionTime=request_data.get('transactionTime'),
            transactionType=request_data.get('transactionType'),
            conductingManner=request_data.get('conductingManner'),
            currencyType=request_data.get('currencyType'),
            minAmountInBirr=request_data.get('minAmountInBirr'),
            maxAmountInBirr=request_data.get('maxAmountInBirr'),
            minAmountInCurrency=request_data.get('minAmountInCurrency'),
            maxAmountInCurrency=request_data.get('maxAmountInCurrency'),
            involverName=request_data.get('involverName'), 
            entityName=request_data.get('entityName'), 
            otherName=request_data.get('otherName'),
            sex=request_data.get('sex'),
            birthDateStart=request_data.get('birthDateStart'),
            birthDateEnd=request_data.get('birthDateEnd'),
            idCardNo=request_data.get('idCardNo'),
            passportNo=request_data.get('passportNo'),
            passportIssuedBy=request_data.get('passportIssuedBy'),
            residenceCountry=request_data.get('residenceCountry'),
            originCountry=request_data.get('originCountry'),
            occupation=request_data.get('occupation'),
            country=request_data.get('country'),
            region=request_data.get('region'),
            city=request_data.get('city'),
            subCity=request_data.get('subCity'),
            woreda=request_data.get('woreda'),
            houseNo=request_data.get('houseNo'),
            postalCode=request_data.get('postalCode'),
            businessMobileNo=request_data.get('businessMobileNo'),
            bussinessTelNo=request_data.get('bussinessTelNo'),
            businessFaxNo=request_data.get('businessFaxNo'),
            residenceTelNo=request_data.get('residenceTelNo'),
            emailAddress=request_data.get('emailAddress'),
            accountNo=request_data.get('accountNo'),
            accHolderBranch=request_data.get('accHolderBranch'),
            accOwnerName=request_data.get('accOwnerName'),
            accountType=request_data.get('accountType'),
            openedDateStart=request_data.get('openedDateStart'),
            openedDateEnd=request_data.get('openedDateEnd'),
            minBalanceHeld=request_data.get('minBalanceHeld'),
            maxBalanceHeld=request_data.get('maxBalanceHeld'),
            balanceHeldDateStart=request_data.get('balanceHeldDateStart'),
            balanceHeldDateEnd=request_data.get('balanceHeldDateEnd'),
            closedDateStart=request_data.get('closedDateStart'),
            closedDateEnd=request_data.get('closedDateEnd'),
            bussinessType=request_data.get('bussinessType'), 
            registrationNo=request_data.get('registrationNo'), 
            taxIdNumber=request_data.get('taxIdNumber'), 
            taxIdIssuedBy=request_data.get('taxIdIssuedBy'), 
            benFullName=request_data.get('benFullName'),
            benAccountNo=request_data.get('benAccountNo'),
            benBranchID=request_data.get('benBranchID'),
            benBranchName=request_data.get('benBranchName'),
            benOwnerEntity=request_data.get('benOwnerEntity'),
            benCountry=request_data.get('benCountry'),
            benRegion=request_data.get('benRegion'),
            benCity=request_data.get('benCity'),
            benZone=request_data.get('benZone'),
            benWoreda=request_data.get('benWoreda'),
            benHouseNo=request_data.get('benHouseNo'),
            benTelNo=request_data.get('benTelNo'),
            benIsEntity=request_data.get('benIsEntity'),
            entity_Name=request_data.get('entity_Name'), 
            typeOfConducting=request_data.get('typeOfConducting'), 
            reportDateStart=request_data.get('reportDateStart'),
            reportDateEnd=request_data.get('reportDateEnd'),
            reportStatus=request_data.get('reportStatus'), 
            reason=request_data.get('reason'), 
            fullName=request_data.get('fullName'),
            page=request_data.get('page', 0),
            size=request_data.get('size', 10),
            requireTotalSize=request_data.get('requireTotalSize')
        )

        paged_response = transaction_service.search_transactions(search_request)
        return jsonify(paged_response.to_dict()), 200

    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body.")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"An error occurred during search: {e}", exc_info=True)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

if __name__ == "__main__":
    
    app.run(host='0.0.0.0', port=10004, debug=True) 
