# CDM-FXSWAP

An example of generating CDM v6 for an FX Swap using Python.

## Instructions

1. [Recommended] - create and activate a virtual Python enviroment

2. Install the Python libraries in the resources directory

```sh
python -m pip install resources/rune_runtime-1.0.18-py3-none-any.whl resources/python_cdm-0.0.0-py3-none-any.whl
```

3. execute create_fx_swap.py.  This will same the BusinessEvent in JSON format.  Set DEBUG to be true to see the results of all validations.  otherwise only errors are reported.


```sh
python src/create_fx_swap.py
```

## Notes

The CDM implementation has overridden sections in _bundle.py

1. Replace condition_2_InterestRateObservable in class cdm_observable_asset_PriceQuantity with the following (keep the adornment @rune_condition):

```python

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
```

2. replace def condition_0_CurrencyExists(self) in class cdm_base_staticdata_asset_common_Cash with the following (keep the adornment @rune_condition):

```python
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
```
3. replace _ALLOWED_METADATA = {'@key', '@key:external'} in class cdm_base_staticdata_party_Party with the following:
```python
    _ALLOWED_METADATA = {'@key', '@key:external', '@ref', '@ref:external'} # over ridden
 ```
