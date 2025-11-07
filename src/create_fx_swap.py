'''demo of creating an fx swap trade using the python implementation of CDM v6
created by FT Advisory info@ftadvisory.co
v1.0, 06-Nov-2025
License: Apache License 2.0

set DEBUG to be true to see the results of all validations.  otherwise only errors are reported.

the CDM implementation has overridden sections in _bundle.py

replace condition_2_InterestRateObservable in class cdm_observable_asset_PriceQuantity with the following (keep the adornment @rune_condition):

    def condition_2_InterestRateObservable(self):
        """
        When the observable is an interest rate index, the price type must be interest rate and the arithmetic operator must be specified.
        """
        # over ridden 
        if getattr(self, "observable") and getattr(self.observable, "Index") and getattr(self.observable,Index.InterestRateIndex) and getattr(self,"price"):
            for p in self.price:
                if p.PriceType == PriceTypeEnum.InterestRate and getattr(p, "arithmeticOperator"):
                    return True
            return False
        else:
            return True

replace def condition_0_CurrencyExists(self) in class cdm_base_staticdata_asset_common_Cash with the following (keep the adornment @rune_condition):

    def condition_0_CurrencyExists(self): 
        """
        There must be one and only one currency code and it must be valid (ie in the enumerated list).
        """
        # over ridden
        ccy_ids = [
            ident for ident in (self.identifier or []) if ident.identifierType == cdm.base.staticdata.asset.common.AssetIdTypeEnum.AssetIdTypeEnum.CURRENCY_CODE
        ]
        # Must be exactly one
        if len(ccy_ids) != 1:
            return False

        code = str(ccy_ids[0].identifier)
        # Match by enum NAME (e.g., "USD", "EUR")
        return code in cdm.base.staticdata.asset.common.CurrencyCodeEnum.CurrencyCodeEnum.__members__

replace _ALLOWED_METADATA = {'@key', '@key:external'} in class cdm_base_staticdata_party_Party with the following:
    _ALLOWED_METADATA = {'@key', '@key:external', '@ref', '@ref:external'} # over ridden
    
the program will create a json file with the business events
'''


import datetime
from decimal import Decimal
from pathlib import Path
from cdm.base.datetime.AdjustableDate import AdjustableDate
from cdm.base.datetime.AdjustableOrRelativeDate import AdjustableOrRelativeDate
from cdm.base.math.NonNegativeQuantitySchedule import NonNegativeQuantitySchedule
from cdm.base.math.UnitType import UnitType
from cdm.base.staticdata.asset.common.Asset import Asset
from cdm.base.staticdata.asset.common.AssetIdentifier import AssetIdentifier as CommonAssetIdentifier
from cdm.base.staticdata.asset.common.AssetIdTypeEnum import AssetIdTypeEnum
from cdm.base.staticdata.asset.common.Cash import Cash
from cdm.base.staticdata.asset.common.ProductIdentifier import ProductIdentifier
from cdm.base.staticdata.asset.common.ProductIdTypeEnum import ProductIdTypeEnum
from cdm.base.staticdata.asset.common.ProductTaxonomy import ProductTaxonomy
from cdm.base.staticdata.asset.common.TaxonomySourceEnum import TaxonomySourceEnum
from cdm.base.staticdata.identifier.AssignedIdentifier import AssignedIdentifier
from cdm.base.staticdata.identifier.Identifier import Identifier
from cdm.base.staticdata.identifier.TradeIdentifierTypeEnum import TradeIdentifierTypeEnum
from cdm.base.staticdata.party.Counterparty import Counterparty
from cdm.base.staticdata.party.CounterpartyRoleEnum import CounterpartyRoleEnum
from cdm.base.staticdata.party.LegalEntity import LegalEntity
from cdm.base.staticdata.party.Party import Party
from cdm.base.staticdata.party.PartyIdentifier import PartyIdentifier
from cdm.base.staticdata.party.PartyRole import PartyRole
from cdm.base.staticdata.party.PartyRoleEnum import PartyRoleEnum
from cdm.base.staticdata.party.PayerReceiver import PayerReceiver
from cdm.event.common.BusinessEvent import BusinessEvent
from cdm.event.common.ExecutionDetails import ExecutionDetails
from cdm.event.common.ExecutionInstruction import ExecutionInstruction
from cdm.event.common.ExecutionTypeEnum import ExecutionTypeEnum
from cdm.event.common.Instruction import Instruction
from cdm.event.common.PrimitiveInstruction import PrimitiveInstruction
from cdm.event.common.State import State
from cdm.event.common.Trade import Trade
from cdm.event.common.TradeIdentifier import TradeIdentifier
from cdm.event.common.TradeState import TradeState
from cdm.event.position.PositionStatusEnum import PositionStatusEnum
from cdm.event.workflow.EventTimestampQualificationEnum import EventTimestampQualificationEnum
from cdm.event.workflow.Workflow import Workflow
from cdm.observable.asset.Observable import Observable
from cdm.observable.asset.Price import Price
from cdm.observable.asset.PriceExpressionEnum import PriceExpressionEnum
from cdm.observable.asset.PriceQuantity import PriceQuantity
from cdm.observable.asset.PriceTypeEnum import PriceTypeEnum
from cdm.product.common.settlement.PhysicalSettlementPeriod import PhysicalSettlementPeriod
from cdm.product.common.settlement.PhysicalSettlementTerms import PhysicalSettlementTerms
from cdm.product.common.settlement.ResolvablePriceQuantity import ResolvablePriceQuantity
from cdm.product.common.settlement.SettlementDate import SettlementDate
from cdm.product.common.settlement.SettlementTerms import SettlementTerms
from cdm.product.common.settlement.SettlementTypeEnum import SettlementTypeEnum
from cdm.product.template.EconomicTerms import EconomicTerms
from cdm.product.template.NonTransferableProduct import NonTransferableProduct
from cdm.product.template.Payout import Payout
from cdm.product.template.SettlementPayout import SettlementPayout
from cdm.product.template.TradeLot import TradeLot
from cdm.product.template.Underlier import Underlier
from pydantic import ValidationError
from rune.runtime.conditions import ConditionViolationError
from rune.runtime.metadata import DateWithMeta, StrWithMeta
from rune.runtime.base_data_class import BaseDataClass

DEBUG = False

TRADE_ID = StrWithMeta("XXX.N1234578")
TRADE_TYPE = StrWithMeta("New Booking")
PRODUCT_ID = StrWithMeta("FX_SWAP")
EXECUTION_TYPE = StrWithMeta("OFF_FACILITY")
EXECUTION_ENTITY = StrWithMeta("OTC")
TRADE_DATE = datetime.datetime(2025, 10, 30) 
SPOT_SETTLE_DATE = datetime.datetime(2025, 11, 4) 
FWD_SETTLE_DATE = datetime.datetime(2025,11,28) 
SPOT_EU_AMT = 2796548.83 
FWD_EU_AMT = 2799994.17 
JPY_AMT_SPOT = 500000000 
JPY_AMT_FWD = 500000000 
PARTY1_NAME = StrWithMeta("BigBank")
PARTY2_NAME = StrWithMeta("Client") 
SPOT_FX_RATE = 178.7918 
FWD_FX_RATE = 178.5718 
CURRENCY = StrWithMeta("EURJPY")
BASE_FX = StrWithMeta("EUR")
TRANSACTION_UTI = StrWithMeta("xxx-abcdef")

def create_price_quantity(fx_rate: float,fx_quantity: float) -> PriceQuantity:
    '''create price quantity'''
    unit=UnitType(currency=BASE_FX,capacityUnit=None,weatherUnit=None,financialUnit=None)
    per_unit_of=UnitType(currency=BASE_FX, capacityUnit= None, weatherUnit=None,financialUnit=None)
    price = Price(value=Decimal(fx_rate),
                  unit=unit,
                  datedValue=None,
                  perUnitOf=per_unit_of,
                  priceType=PriceTypeEnum.EXCHANGE_RATE,
                  composite=None,
                  arithmeticOperator=None,
                  cashPrice=None,
                  priceExpression=PriceExpressionEnum.ABSOLUTE_TERMS)
    quantity = NonNegativeQuantitySchedule(value = Decimal(fx_quantity),
                                           unit=per_unit_of,
                                           datedValue=None,
                                           multiplier=None,
                                           frequency=None)
    price_quantity = PriceQuantity(price=[price],
                                   quantity=[quantity],
                                   observable=None,
                                   effectiveDate=None)
    validate_pydantic_object(price_quantity)
    return price_quantity

def create_resolvable_price_quantity(fx_rate: float,fx_quantity: float)-> ResolvablePriceQuantity:
    '''create the price quantity from the rate and fx quantity'''
    unit=UnitType(currency=CURRENCY,capacityUnit=None,weatherUnit=None,financialUnit=None)
    per_unit_of=UnitType(currency=BASE_FX, capacityUnit= None, weatherUnit=None,financialUnit=None)

    price = Price(value=Decimal(fx_rate),
                  unit=unit,
                  datedValue=None,
                  perUnitOf=per_unit_of,
                  priceType=PriceTypeEnum.EXCHANGE_RATE,
                  composite=None,
                  arithmeticOperator=None,
                  cashPrice=None,
                  priceExpression=PriceExpressionEnum.ABSOLUTE_TERMS)
    validate_pydantic_object(price)
    quantity = NonNegativeQuantitySchedule(value = Decimal(fx_quantity),
                                           unit=per_unit_of,
                                           datedValue=None,
                                           multiplier=None,
                                           frequency=None)
    validate_pydantic_object(quantity)
    price_quantity = ResolvablePriceQuantity (resolvedQuantity=None,
                                              quantitySchedule=quantity,
                                              quantityReference = None,
                                              quantityMultiplier=None,
                                              reset=None,
                                              futureValueNotional=None,
                                              priceSchedule=[price])
    validate_pydantic_object(price_quantity)
    return price_quantity
def create_settlement_terms(settle_date: datetime.datetime)->SettlementTerms:
    '''create setttlement terms'''
    settlement_date=SettlementDate(valueDate=settle_date,
                                   adjustableOrRelativeDate=None,
                                   adjustableDates=None,
                                   businessDateRange=None,
                                   cashSettlementBusinessDays=None,
                                   paymentDelay=None)
    physical_settlement_period = PhysicalSettlementPeriod(businessDaysNotSpecified = None,
                                                          businessDays=0,maximumBusinessDays = None)
    physical_settlement_terms = PhysicalSettlementTerms(clearedPhysicalSettlement = None,
                                                        predeterminedClearingOrganizationParty = None,
                                                        physicalSettlementPeriod=physical_settlement_period,
                                                        deliverableObligations = None,
                                                        escrow = None,
                                                        sixtyBusinessDaySettlementCap = None)
    settlement_terms=SettlementTerms(settlementType=SettlementTypeEnum.PHYSICAL,
                                     transferSettlementType=None,
                                     settlementCurrency=None, #??
                                     settlementDate=settlement_date,
                                     settlementCentre=None,
                                     settlementProvision=None,
                                     cashSettlementTerms=None,
                                     standardSettlementStyle=None,
                                     physicalSettlementTerms=physical_settlement_terms)
    return settlement_terms

def create_settlement_payout (payer_receiver: PayerReceiver,
                   fx_rate: float,
                   fx_quantity: float,
                   settle_date: datetime.datetime,
                   observable: Observable)->Payout:
    '''create a settlement payout - resused for spot and fwd'''
    price_quantity = create_resolvable_price_quantity(fx_rate=fx_rate, fx_quantity=fx_quantity)
    settlement_terms = create_settlement_terms(settle_date=settle_date)
    underlier = Underlier(Observable=observable, Product=None)
    validate_pydantic_object(underlier)
    settlement_payout = SettlementPayout(payerReceiver=payer_receiver,
                                         priceQuantity=price_quantity,
                                         principalPayment=None,
                                         settlementTerms=settlement_terms,
                                         underlier=underlier,
                                         deliveryTerm=None,
                                         delivery=None,
                                         schedule=None)
    validate_pydantic_object(settlement_payout)
    payout = Payout(AssetPayout=None,
                    CommodityPayout=None,
                    CreditDefaultPayout=None,
                    FixedPricePayout=None,
                    InterestRatePayout=None,
                    OptionPayout=None,
                    PerformancePayout=None,
                    SettlementPayout=settlement_payout)
    validate_pydantic_object(payout)
    return payout

def create_trade_business_event() -> BusinessEvent:
    '''create a business event to match the trade'''
    trade_id = TradeIdentifier(assignedIdentifier=[AssignedIdentifier(identifier=TRANSACTION_UTI, version=None)],
                               identifierType=TradeIdentifierTypeEnum.UNIQUE_TRANSACTION_IDENTIFIER,
                               issuerReference=None,
                               issuer="NA")
    validate_pydantic_object(trade_id)
    product_id = ProductIdentifier(identifier=PRODUCT_ID, source=ProductIdTypeEnum.NAME)
    validate_pydantic_object(product_id)
    assinged_id = CommonAssetIdentifier(identifier=StrWithMeta(BASE_FX), 
                                        identifierType=AssetIdTypeEnum.CURRENCY_CODE)
    validate_pydantic_object(assinged_id)
    cash = Cash(identifier=[assinged_id],
                taxonomy=None,
                isExchangeListed=None,
                exchange=None,
                relatedExchange=None)
    validate_pydantic_object(cash)
    asset = Asset(Cash=cash,
                  Commodity=None,
                  DigitalAsset=None,
                  Instrument=None)
    validate_pydantic_object(asset)
    observable = Observable(Asset=asset, Basket=None, Index=None)
    validate_pydantic_object(observable)
    spot_payer_receiver = PayerReceiver(payer=CounterpartyRoleEnum.PARTY_1,receiver=CounterpartyRoleEnum.PARTY_2)
    spot_payout=create_settlement_payout (payer_receiver=spot_payer_receiver,
                                          fx_rate=SPOT_FX_RATE,
                                          fx_quantity=SPOT_EU_AMT,
                                          settle_date=SPOT_SETTLE_DATE,
                                          observable=observable)
    fwd_payer_receiver = PayerReceiver(payer=CounterpartyRoleEnum.PARTY_2,receiver=CounterpartyRoleEnum.PARTY_1)
    fwd_payout=create_settlement_payout (payer_receiver=fwd_payer_receiver,
                                         fx_rate=FWD_FX_RATE,
                                         fx_quantity=FWD_EU_AMT,
                                         settle_date=FWD_SETTLE_DATE,
                                         observable=observable)
    spot_settle_date = AdjustableDate(unadjustedDate=None,
                                      dateAdjustments=None,
                                      dateAdjustmentsReference=None,
                                      adjustedDate=DateWithMeta(SPOT_SETTLE_DATE))
    effective_date = AdjustableOrRelativeDate(adjustableDate=spot_settle_date, relativeDate=None)
    fwd_settle_date = AdjustableDate(unadjustedDate=None,
                                     dateAdjustments=None,
                                     dateAdjustmentsReference=None,
                                     adjustedDate=DateWithMeta(FWD_SETTLE_DATE))
    termination_date=AdjustableOrRelativeDate(adjustableDate=fwd_settle_date, relativeDate=None)
    economic_terms = EconomicTerms(effectiveDate=effective_date,
                                   terminationDate=termination_date,
                                   dateAdjustments=None,
                                   payout=[spot_payout, fwd_payout],
                                   terminationProvision=None,
                                   calculationAgent=None,
                                   nonStandardisedTerms=None,
                                   collateral=None)
    validate_pydantic_object(economic_terms)
    taxonomy = [ProductTaxonomy(source=TaxonomySourceEnum.ISDA,
                                value=None,
                                primaryAssetClass=None,
                                secondaryAssetClass=None,
                                productQualifier="ForeignExchange_Spot_Forward")]
    product = NonTransferableProduct (identifier=None, taxonomy = taxonomy,economicTerms = economic_terms)
    validate_pydantic_object(product)
    execution_details = ExecutionDetails(executionType=ExecutionTypeEnum[EXECUTION_TYPE],
                                         executionVenue=LegalEntity(entityId=[EXECUTION_ENTITY], name=EXECUTION_ENTITY),
                                         packageReference=None)
    validate_pydantic_object(execution_details)
    spot_price_quantity = create_price_quantity(fx_rate=SPOT_FX_RATE,fx_quantity=SPOT_EU_AMT)
    fwd_price_quantity = create_price_quantity(fx_rate=FWD_FX_RATE,fx_quantity=FWD_EU_AMT)
    parties = [Party(name=PARTY1_NAME,
                     partyId=[PartyIdentifier(identifier=PARTY1_NAME, identifierType=None)],
                     businessUnit=None,
                     person=None,
                     personRole=None,
                     account=None,
                     contactInformation=None),
               Party(name=PARTY2_NAME, 
                     partyId=[PartyIdentifier(identifier=PARTY2_NAME, identifierType=None)],
                     businessUnit=None,
                     person=None,
                     personRole=None,
                     account=None,
                     contactInformation=None)
               ]
    validate_pydantic_list (parties)
    party_roles = [PartyRole(partyReference=parties[0],
                             role=PartyRoleEnum.BUYER,
                             ownershipPartyReference=None),
                   PartyRole(partyReference=parties[1],
                             role=PartyRoleEnum.SELLER,
                             ownershipPartyReference=None)
                   ]
    validate_pydantic_list (party_roles)
    counterparties = [Counterparty(role=CounterpartyRoleEnum.PARTY_1,partyReference=parties[0]),
                      Counterparty(role=CounterpartyRoleEnum.PARTY_2,partyReference=parties[1])]
    validate_pydantic_list(counterparties)
    execution_instruction = ExecutionInstruction(product=product,
                                                 priceQuantity=[spot_price_quantity,fwd_price_quantity],
                                                 counterparty=counterparties,
                                                 parties=parties,
                                                 partyRoles=party_roles,
                                                 executionDetails=execution_details,
                                                 tradeDate=DateWithMeta(TRADE_DATE),
                                                 tradeIdentifier=[trade_id],
                                                 ancillaryParty=None,
                                                 tradeTime=None,
                                                 collateral=None,
                                                 lotIdentifier=None)
    validate_pydantic_object(execution_instruction)
    primitive_instruction = PrimitiveInstruction(contractFormation = None,
                                                 execution=execution_instruction,
                                                 exercise = None,
                                                 partyChange = None,
                                                 quantityChange = None,
                                                 reset = None,
                                                 split  = None,
                                                 termsChange = None,
                                                 transfer = None,
                                                 indexTransition = None,
                                                 stockSplit = None,
                                                 observation = None,
                                                 valuation = None)
    validate_pydantic_object(primitive_instruction)
    instruction = Instruction(primitiveInstruction=primitive_instruction, before=None)
    validate_pydantic_object(instruction)
    event = BusinessEvent(intent = None,
                          corporateActionIntent  = None,
                          eventDate=TRADE_DATE,
                          effectiveDate=TRADE_DATE,
                          packageInformation = None,
                          instruction=[instruction],
                          eventQualifier = None,
                          after = None)
    # event.after would be set by executing the deacti
    return event if validate_pydantic_object(event) else None

def extract_info_from_event (event: BusinessEvent) -> dict:
    '''extract info from an event'''
    results = {}
    if event is not None and event.instruction and getattr(event, "instruction") and len(event.instruction) > 0 and event.instruction[0].primitiveInstruction:
        execution_instruction = event.instruction[0].primitiveInstruction.execution
        if execution_instruction:
            results['trade_date'] = execution_instruction.tradeDate
            results['trade_id'] = Identifier(issuer=execution_instruction.tradeIdentifier[0].issuer,
                                            assignedIdentifier=execution_instruction.tradeIdentifier[0].assignedIdentifier,
                                            issuerReference=execution_instruction.tradeIdentifier[0].issuerReference)
            results['price_quantity'] = execution_instruction.priceQuantity
            results['product'] = execution_instruction.product
            results['counterparty'] = execution_instruction.counterparty
            results['tradeIdentifier'] = execution_instruction.tradeIdentifier
            results['parties'] = execution_instruction.parties
            results['partyRoles'] = execution_instruction.partyRoles,
            results['executionDetails'] = execution_instruction.executionDetails
            results['economic_terms'] = execution_instruction.product.economicTerms
            
    return results

def validate_pydantic_list (list_of_objs) -> bool:
    '''validate a list of pydantic objects'''
    result = True
    for o in list_of_objs:
        result = result and validate_pydantic_object(o)
    return result

def validate_pydantic_object (obj) -> bool:
    '''validate pydantic objects'''
    try:
        obj.validate_model()
        if (DEBUG):
            print(f"validating type {type(obj)}...successful")
        return True
    except (ConditionViolationError, ValidationError) as e:
        print(f"validating type {type(obj)}...failed")
        print(e)
        return False

def main ():
    '''main'''
    print('creating business event')
    event = create_trade_business_event()
    print('writing business event')
    Path("fx_swap_business_event.json").write_text(event.rune_serialize(indent=3), encoding="utf-8")
    event_json = Path("fx_swap_business_event.json").read_text()
    event_new = BaseDataClass.rune_deserialize (event_json)
    results = extract_info_from_event(event)
    print('trade date:', results['trade_date'])
    print('effective date:', results['economic_terms'].effectiveDate.adjustableDate.adjustedDate)
    print('termination date:', results['economic_terms'].terminationDate.adjustableDate.adjustedDate)
    print('payer receiver 0:',results['economic_terms'].payout[0].SettlementPayout.payerReceiver.payer)
    print('payer receiver 1:',results['economic_terms'].payout[1].SettlementPayout.payerReceiver.payer)
    pay_idx = 0 if results['economic_terms'].payout[0].SettlementPayout.payerReceiver.payer == CounterpartyRoleEnum.PARTY_1 else 1
    rec_idx = 1 if pay_idx == 0 else 0
    print('first leg payer: ', results['parties'][pay_idx].name, 'receiver:', results['parties'][rec_idx].name)
    print('first leg exchange rate:', f"{results['economic_terms'].payout[0].SettlementPayout.priceQuantity.priceSchedule[0].value:.3f}",
          ' currency:',results['economic_terms'].payout[0].SettlementPayout.priceQuantity.priceSchedule[0].unit.currency,
          ' base currency:',results['economic_terms'].payout[0].SettlementPayout.priceQuantity.priceSchedule[0].perUnitOf.currency)
    print('first leg amt:',f"{results['economic_terms'].payout[0].SettlementPayout.priceQuantity.quantitySchedule.value:,.2f}",
          ' currency:', results['economic_terms'].payout[0].SettlementPayout.priceQuantity.quantitySchedule.unit.currency)
    print('first leg settlement date:', results['economic_terms'].payout[0].SettlementPayout.settlementTerms.settlementDate.valueDate)
    pay_idx = 0 if results['economic_terms'].payout[1].SettlementPayout.payerReceiver.payer == 'Payer1' else 1
    rec_idx = 1 if pay_idx == 0 else 0
    print('second leg payer: ', results['parties'][pay_idx].name, 'receiver:', results['parties'][rec_idx].name)
    print('second leg exchange rate:', f"{results['economic_terms'].payout[1].SettlementPayout.priceQuantity.priceSchedule[0].value:.3f}",
          ' currency:',results['economic_terms'].payout[1].SettlementPayout.priceQuantity.priceSchedule[0].unit.currency,
          ' base currency:',results['economic_terms'].payout[1].SettlementPayout.priceQuantity.priceSchedule[0].perUnitOf.currency)
    print('second leg amt:',f"{results['economic_terms'].payout[1].SettlementPayout.priceQuantity.quantitySchedule.value:,.2f}",
          ' currency:', results['economic_terms'].payout[1].SettlementPayout.priceQuantity.quantitySchedule.unit.currency)
    print('second leg settlement date:', results['economic_terms'].payout[1].SettlementPayout.settlementTerms.settlementDate.valueDate)

if __name__ == "__main__":
    main()
# EOF