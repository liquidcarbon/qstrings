Given dataset defined by this class, return a query showing elements with 8 or more isotopes.

```python
class IsotopeData(af.Dataset):
    symbol = af.VectorObject("Element")
    number = af.VectorI8("Atomic Number (Z)")
    mass = af.VectorF64("Isotope Mass (Da)")
    abundance = af.VectorF64("Relative natural abundance")
```

Return only raw query - no markdown, no backquotes, from clause must contain '{URL}' in single quotes, put reasoning and any other text into "/*...*/" comments.