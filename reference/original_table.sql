/*
Contract Activities
-------------------
New Revenue = Revenue in period (t) > 0. T is the first period customer shows up in
Retained Revenue = Revenue in period (t) = Revenue last period (t-1)
Expansion Revenue = Revenue in period (t) > Revenue last period (t-1)
Contraction Revenue = Revenue in period (t) < Revenue last period (t-1)
Churned revenue = Revenue in period (t) = 0. Revenue last period (t-1) > 0

If a customer spent $12 last period (t-1) and then spent:
- Spent $12 this period (t) => insert $12 retained revenue
- Spent $15 this period (t) => insert $12 retained revenue + $3 expansion revenue
- Spent $10 this period (t) => insert $10 retained revenue + $2 contraction revenue
- Spent $0  this period (t) => insert $12 churned revenue
*/