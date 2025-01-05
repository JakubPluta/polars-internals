from datetime import date

import polars as pl
from _const import COOKBOOK_DATA_DIR
import polars.selectors as cs

df = pl.read_csv(COOKBOOK_DATA_DIR / "contoso_sales.csv", try_parse_dates=True)
print(df.head(1))


ctx = pl.SQLContext(eager=True)
ctx.register("sales", df)
print(ctx.execute("SELECT `Customer Name`, Brand, Category FROM sales LIMIT 5"))

print(
    ctx.execute(
        """
      select
        Brand,
        avg(Quantity) as `Avg Quantity` 
      from sales
      group by 
        Brand
      order by 
        `Avg Quantity` desc
      limit 5
    """
    )
)

# using lazy frame in sql context
with pl.SQLContext(lf=df.lazy(), eager=False) as ctx:
    q = ctx.execute(
        """
      select
        Brand,
        avg(Quantity) as `Avg Quantity` 
      from lf
      group by 
        Brand
      order by 
        `Avg Quantity` desc
      limit 5
    """
    )
    print(q.collect())
