from django import forms

stocks =[("AAPL","AAPL"), ("ABNB","ABNB"), ("ADBE","ADBE"), ("ADI","ADI"), ("ADP","ADP"), ("ADSK","ADSK"), ("AEP","AEP"), ("ALGN","ALGN"), ("AMAT","AMAT"), ("AMD","AMD"), ("AMGN","AMGN"), ("AMZN","AMZN"), ("ANSS","ANSS"), ("ASML","ASML"), ("ATVI","ATVI"), ("AVGO","AVGO"), ("AZN","AZN"), ("BIDU","BIDU"), ("BIIB","BIIB"), ("BKNG","BKNG"), ("CDNS","CDNS"), ("CEG","CEG"), ("CHTR","CHTR"), ("CMCSA","CMCSA"), ("COST","COST"), ("CPRT","CPRT"), ("CRWD","CRWD"), ("CSCO","CSCO"), ("CSX","CSX"), ("CTAS","CTAS"), ("CTSH","CTSH"), ("DDOG","DDOG"), ("DLTR","DLTR"), ("DOCU","DOCU"), ("DXCM","DXCM"), ("EA","EA"), ("EBAY","EBAY"), ("EXC","EXC"), ("FAST","FAST"), ("FISV","FISV"), ("FTNT","FTNT"), ("GILD","GILD"), ("GOOG","GOOG"), ("GOOGL","GOOGL"), ("HON","HON"), ("IDXX","IDXX"), ("ILMN","ILMN"), ("INTC","INTC"), ("INTU","INTU"), ("ISRG","ISRG"), ("JD","JD"), ("KDP","KDP"), ("KHC","KHC"), ("KLAC","KLAC"), ("LCID","LCID"), ("LRCX","LRCX"), ("LULU","LULU"), ("MAR","MAR"), ("MCHP","MCHP"), ("MDLZ","MDLZ"), ("MELI","MELI"), ("META","META"), ("MNST","MNST"), ("MRNA","MRNA"), ("MRVL","MRVL"), ("MSFT","MSFT"), ("MTCH","MTCH"), ("MU","MU"), ("NFLX","NFLX"), ("NTES","NTES"), ("NVDA","NVDA"), ("NXPI","NXPI"), ("ODFL","ODFL"), ("OKTA","OKTA"), ("ORLY","ORLY"), ("PANW","PANW"), ("PAYX","PAYX"), ("PCAR","PCAR"), ("PDD","PDD"), ("PEP","PEP"), ("PYPL","PYPL"), ("QCOM","QCOM"), ("REGN","REGN"), ("ROST","ROST"), ("SBUX","SBUX"), ("SGEN","SGEN"), ("SIRI","SIRI"), ("SNPS","SNPS"), ("SPLK","SPLK"), ("SWKS","SWKS"), ("TEAM","TEAM"), ("TMUS","TMUS"), ("TSLA","TSLA"), ("TXN","TXN"), ("VRSK","VRSK"), ("VRSN","VRSN"), ("VRTX","VRTX"), ("WBA","WBA"), ("WDAY","WDAY"), ("XEL","XEL"), ("ZM","ZM"), ("ZS","ZS")]

class DateForm(forms.Form):
    ticker = forms.CharField(widget=forms.Select(choices=stocks))
    date = forms.DateField(widget = forms.DateInput(attrs={'type':'date'}))


    
    
