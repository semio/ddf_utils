# ddf--gapminder--co2_emission

CO2 Emission dataset by Gapminder

## Indicators

- yearly_co2_emissions_1000_tonnes: Yearly CO2 emissions (1000 tonnes)
- co2_emissions_tonnes_per_person: CO2 per capita (tonnes per person)
- cumulative_co2_emissions_tonnes: Cumulative CO2 emissions (tonnes)

## Definition of indicator

**co2_emissions_tonnes_per_person**

name: CO2 per capita

description: Per capita carbon dioxide emissions from the fossil fuel consumption, cement production and gas flaring (metric tons of CO2 per person).

**yearly_co2_emissions_1000_tonnes**

name: Yearly CO2 emissions

description: Total carbon dioxide emissions from the fossil fuel consumption, cement production and gas flaring during the given year (1000 metric tons of CO2).

**cumulative_co2_emissions_tonnes**

name: Cumulative CO2 emissions

description: Cumulative carbon dioxide emissions from fossil fuel consumption, cement production and gas flaring for the period between 1751 and the relevant year (metric tons of CO2).

## Unit of measurement

- co2_emissions_tonnes_per_person: metric tons per person
- yearly_co2_emissions_1000_tonnes: 1000 metric tons
- cumulative_co2_emissions_tonnes: metric tons

## Versions

Current version: 20161111

### Revision history

#### 20161111

merge and split certain countries to match Gapminder's geo domain. More detains in #1

#### 20161108

first version of dataset

## Data sources summary

- population from Gapminder's population estimation: https://github.com/open-numbers/ddf--gapminder--population
- co2 emission data from CDIAC: https://github.com/open-numbers/ddf--cdiac--co2

## Specific information about indicators

### CO2 per capita

The CO2 per capita time series in the CDIAC dataset starts at 1950, since
["population estimates were not available [...] before 1950"](http://cdiac.ornl.gov/ftp/ndp030/global.1751_2013.ems)
to CDIAC. Most likely they use the [UN population estimates](https://esa.un.org/unpd/wpp/).
Note: CDIAC Total CO2 emissions time series does go back to 1751.
The CO2 per capita time series in this dataset starts at 1751, since Gapminder uses its own
population estimates referenced above, which contains estimates back to 1086 for certain
countries. CO2 per capita is calculated and included when both Total CO2 emissions (CDIAC)
and population (Gapminder) estimates are available for a country in a certain year.
