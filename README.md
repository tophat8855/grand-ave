# Grand Ave Crash Data Analysis

This Clay Notebook project analyzes California's crash data (CCRS) with a focus on Oakland's Grand Ave and Telegraph Ave. The project demonstrates practical techniques for working with real-world GIS data in Clojure, including handling missing coordinates, messy street names, and spatial matching.

**🔗 [View the Live Site](https://tophat8855.github.io/grand-ave/)** (GitHub Pages)

## What's Inside

- **[Tutorial](docs/tutorial.html)**: Comprehensive guide to GIS data handling techniques used in this project
- **[Main Analysis](docs/index.html)**: Full crash analysis for Grand Ave and Telegraph Ave with interactive maps

## Key GIS Techniques Demonstrated

This project shows how to handle common real-world GIS challenges:

1. **Missing Coordinates**: Derive locations from street intersection names using spatial matching
2. **Messy Street Names**: Normalize and clean inconsistent text data
3. **Coordinate System Transformations**: Use local projected coordinates for accurate spatial operations
4. **Spatial Indexing**: R-tree indexes for efficient polygon/point queries
5. **Buffer-Based Matching**: Fuzzy spatial matching for imperfect data

## Project Goals

1. **Process California’s Crash Data (CCRS)**:
   - Analyze trends on Telegraph Ave before and after traffic calming measures were implemented.

2. **Examine Current Crash Patterns on Grand Ave**:
   - Investigate the affected demographics, such as age groups.

3. **Visualize and Interpret Findings**:
   - Create visualizations to help interpret the analyzed data and present the findings in an understandable manner.


## Data Sources

This project combines data from three different sources:

- **California Crash Reporting System (CCRS)**: [data.ca.gov](https://data.ca.gov/dataset/ccrs)
  - Crashes, parties, and injured persons data (2015-2024)
  - Oakland-filtered datasets in `datasets/` directory
- **Alameda County Street Centerlines**: [data.acgov.org](https://data.acgov.org/datasets/da0be53b6d0d44eda6c1d88a799b5fb0/explore)
  - GeoJSON with street geometries for Alameda County
  - Filtered to Oakland city limits (see `notebooks/data.clj`)
- **Oakland Neighborhood Boundaries**: [OpenOakland](https://data.openoakland.org/dataset/neighborhoods-ceda-2002/resource/ef24774e-f0ac-46af-80cb-d87c2bf9b46b)
  - Polygon data for neighborhood assignment (CEDA 2002 neighborhoods)

## Building the Site

This project uses Clay (Clojure notebooks) + Quarto to generate static HTML for GitHub Pages.

### Workflow

1. **Edit notebooks** in `notebooks/` (e.g., `tutorial.clj`, `index.clj`)
2. **Generate QMD**: In Emacs, call `clay-make-ns-html` on the namespace
   - This creates `.qmd` and `.html` files in `docs/`
3. **Build full site**: Run `quarto render` in the `docs/` directory
   - Generates complete site with navigation, search, etc.
4. **Deploy**: Push to GitHub - Pages automatically serves from `/docs`

### Local Development

```bash
# Install dependencies
clj -P

# Generate a single notebook (or use clay-make-ns-html in Emacs)
clj -M:clay ...

# Build the full Quarto site
cd docs
quarto render
```

## Project Structure

```
grand-ave/
├── notebooks/           # Clay/Clojure notebooks
│   ├── tutorial.clj    # GIS methodology tutorial
│   ├── index.clj       # Main analysis
│   ├── data.clj        # Data loading utilities
│   └── locations.clj   # Advanced spatial analysis
├── datasets/           # Oakland crash data CSVs (2015-2024)
├── data/              # GIS reference data (neighborhoods, streets)
├── docs/              # Generated site (GitHub Pages)
└── deps.edn           # Clojure dependencies
```

## Evaluating Telegraph Ave’s Road Diet Impact

    How did crash rates change on Telegraph Ave before and after the lane reduction?

    Did the number of injuries and fatalities decrease after the redesign?

    What types of crashes (e.g., pedestrian-involved, cyclist-involved, rear-end collisions) were most affected?

    Did crash severity change (e.g., more minor crashes, fewer fatal ones)?

## Understanding Grand Ave’s Current Safety Issues

    What are the most common types of crashes on Grand Ave?

    Who is most affected (age groups, pedestrian/cyclist involvement, vehicle occupants)?

    What locations along Grand Ave see the most crashes?

    Are there trends in crash timing (e.g., peak hours, weekends vs. weekdays)?

    How do crash rates on Grand Ave compare to pre-road-diet Telegraph?

## Comparing Telegraph and Grand Ave

    How do total crash numbers compare between the two corridors?

    Are the types of crashes similar, or does one street see different patterns?

    Who is most at risk on each street?

    Based on Telegraph’s data, what safety improvements could be predicted for Grand Ave if a similar lane reduction were implemented?

## Project Goals

This analysis focuses on two main areas:

1. **Telegraph Ave**: Analyze crash trends before and after traffic calming measures (road diet)
   - KONO district (19th-29th St)
   - Pill Hill district (29th-41st St)

2. **Grand Ave**: Examine current crash patterns and affected demographics
   - Harrison to Mandana Boulevard section
   - Focus on pedestrian and bicycle safety

## For the Clojure Data Science Community

This project serves as a practical example of:
- Working with government open data in Clojure
- Handling imperfect geographic data
- Using JTS (Java Topology Suite) through the `geo` library
- Creating interactive visualizations with Clay/Quarto
- Building static data science sites with GitHub Pages

**Key libraries used:**
- `scicloj/noj` - Data science toolkit
- `factual/geo` - GIS operations and JTS wrapper
- `scicloj/tablecloth` - DataFrame operations
- `scicloj/clay` - Notebook generation

## Contributing & Questions

This project was created for a presentation at a Clojure data science conference. If you're working on similar GIS problems in Clojure, feel free to open issues or discussions!

## License

Data is from public government sources. Code examples are provided for educational purposes.
