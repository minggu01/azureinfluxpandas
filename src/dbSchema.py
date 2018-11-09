class DBSchema:
    DBNAME='timeseries'
    RETENTION_POLICY='autogen'
    MEASUREMENT='tsdata'

    TAGKEY_PLANT='plant' # value on this tag shall be the shortname of a plant. Plant.shortName
    TAGKEY_TAG='tag' # the ims tag name, calculated or predicted tag name

    TAGKEY_TAGTYPE='tagtype'
    TAGVALUE_TAGTYPE_IMS='ims'
    TAGVALUE_TAGTYPE_CALCU='calcu'

    TAGKEY_DATATYPE='datatype'
    TAGVALUE_DATATYPE_RAW='raw'
    TAGVALUE_DATATYPE_5MIN_MEAN = '5MinMean'
    TAGVALUE_DATATYPE_5MIN_MAX = '5MinMax'
    TAGVALUE_DATATYPE_5MIN_MIN = '5MinMin'
    TAGVALUE_DATATYPE_5MIN_STDDEV = '5MinStddev'

    FIELDKEY_VALUE='Value'