# Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Reference
Contract Activities
-------------------
New Revenue = Revenue in period (t) > 0. T is the first period customer shows up in
Retained Revenue = Revenue in period (t) = Revenue last period (t-1)
Expansion Revenue = Revenue in period (t) > Revenue last period (t-1)
Contraction Revenue = Revenue in period (t) < Revenue last period (t-1)
Churned revenue = Revenue in period (t) = 0. Revenue last period (t-1) > 0
Resurrected Revenue = Revenue in period (t) > 0. Revenue last period (t-1) = 0. Revenue in past periods (t-1+n) > 0

If a customer spent $12 last period (t-1) and then spent:
- Spent $12 this period (t) => insert $12 retained revenue
- Spent $15 this period (t) => insert $12 retained revenue + $3 expansion revenue
- Spent $10 this period (t) => insert $10 retained revenue + $2 contraction revenue
- Spent $0  this period (t) => insert $12 churned revenue