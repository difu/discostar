# DiscoStar Release Analysis SQL Queries

This document contains SQL queries for analyzing your record collection with focus on identifying original pressings, first presses, and country information.

## 1. Country Information Query

### Purpose
Retrieve country information for both individual releases and their associated master releases.

### SQL Query
```sql
SELECT 
    r.id as release_id,
    r.title as release_title,
    r.country as release_country,
    m.id as master_id,
    m.title as master_title,
    -- For master country, we need to find the main release or most common country
    CASE 
        WHEN m.main_release_id IS NOT NULL THEN (
            SELECT country 
            FROM releases 
            WHERE id = m.main_release_id
        )
        ELSE (
            SELECT country 
            FROM releases 
            WHERE master_id = m.id 
                AND country IS NOT NULL 
            GROUP BY country 
            ORDER BY COUNT(*) DESC 
            LIMIT 1
        )
    END as master_country
FROM releases r
LEFT JOIN masters m ON r.master_id = m.id
WHERE r.country IS NOT NULL
ORDER BY r.title;
```

### Advantages
- ✅ Provides both release and master country information
- ✅ Handles cases where master doesn't have direct country field
- ✅ Uses intelligent fallback (most common country) for master determination
- ✅ Filters out releases with no country data

### Disadvantages
- ❌ May miss releases with NULL country values
- ❌ Master country determination is heuristic, not definitive
- ❌ Performance may be slower due to subqueries

---

## 2. Artist-Specific Query (Dire Straits Example)

### Purpose
Find all releases by a specific artist with release dates and country information from your collection.

### SQL Query
```sql
SELECT 
    r.id as release_id,
    r.title as release_title,
    r.country as release_country,
    r.year as release_year,
    r.released as release_date,
    m.id as master_id,
    m.title as master_title,
    m.year as master_year,
    CASE 
        WHEN m.main_release_id IS NOT NULL THEN (
            SELECT country 
            FROM releases 
            WHERE id = m.main_release_id
        )
        ELSE (
            SELECT country 
            FROM releases 
            WHERE master_id = m.id 
                AND country IS NOT NULL 
            GROUP BY country 
            ORDER BY COUNT(*) DESC 
            LIMIT 1
        )
    END as master_country,
    uc.rating,
    uc.date_added as added_to_collection
FROM releases r
LEFT JOIN masters m ON r.master_id = m.id
INNER JOIN user_collection uc ON r.id = uc.release_id
INNER JOIN users u ON uc.user_id = u.id
WHERE (
    r.artists LIKE '%Dire Straits%' OR 
    r.title LIKE '%Dire Straits%' OR
    m.artists LIKE '%Dire Straits%' OR
    m.title LIKE '%Dire Straits%'
)
ORDER BY r.year, r.released, r.title;
```

### Advantages
- ✅ Collection-specific results only
- ✅ Includes personal collection metadata (rating, date added)
- ✅ Searches both release and master data for artist
- ✅ Chronological ordering

### Disadvantages
- ❌ String matching may miss variations in artist names
- ❌ Could return false positives for compilation albums
- ❌ Requires manual artist name input

---

## 3. First Press Identification (Year-Based)

### Purpose
Identify first presses where release year equals master year, with country matching analysis.

### SQL Query
```sql
SELECT 
    r.id as release_id,
    r.title as release_title,
    r.country as release_country,
    r.year as release_year,
    r.released as release_date,
    m.id as master_id,
    m.title as master_title,
    m.year as master_year,
    -- Determine master country
    CASE 
        WHEN m.main_release_id IS NOT NULL THEN (
            SELECT country 
            FROM releases 
            WHERE id = m.main_release_id
        )
        ELSE (
            SELECT country 
            FROM releases 
            WHERE master_id = m.id 
                AND country IS NOT NULL 
            GROUP BY country 
            ORDER BY COUNT(*) DESC 
            LIMIT 1
        )
    END as master_country,
    -- Check if countries match
    CASE 
        WHEN r.country IS NULL OR (
            CASE 
                WHEN m.main_release_id IS NOT NULL THEN (
                    SELECT country 
                    FROM releases 
                    WHERE id = m.main_release_id
                )
                ELSE (
                    SELECT country 
                    FROM releases 
                    WHERE master_id = m.id 
                        AND country IS NOT NULL 
                    GROUP BY country 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 1
                )
            END
        ) IS NULL THEN 'Unknown'
        WHEN r.country = (
            CASE 
                WHEN m.main_release_id IS NOT NULL THEN (
                    SELECT country 
                    FROM releases 
                    WHERE id = m.main_release_id
                )
                ELSE (
                    SELECT country 
                    FROM releases 
                    WHERE master_id = m.id 
                        AND country IS NOT NULL 
                    GROUP BY country 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 1
                )
            END
        ) THEN 'Match'
        ELSE 'Different'
    END as country_match_status,
    uc.rating,
    uc.date_added as added_to_collection
FROM releases r
INNER JOIN masters m ON r.master_id = m.id
INNER JOIN user_collection uc ON r.id = uc.release_id
INNER JOIN users u ON uc.user_id = u.id
WHERE r.year = m.year  -- First press condition: release year equals master year
    AND r.year IS NOT NULL 
    AND m.year IS NOT NULL
ORDER BY m.year, r.title;
```

### Advantages
- ✅ Simple, clear first press identification logic
- ✅ Includes country matching analysis
- ✅ Collection-specific results
- ✅ Handles missing data gracefully

### Disadvantages
- ❌ Excludes releases missing year data (could be originals)
- ❌ Year-only matching may miss legitimate first presses with data inconsistencies
- ❌ Doesn't consider other indicators (notes, labels, formats)

---

## 4. Advanced Multi-Factor Originality Analysis

### Purpose
Comprehensive analysis using multiple factors to assess originality likelihood.

### SQL Query
```sql
SELECT 
    r.id as release_id,
    r.title,
    r.year as release_year,
    m.year as master_year,
    r.country,
    
    -- Year comparison
    CASE WHEN r.year = m.year THEN 1 ELSE 0 END as same_year_score,
    
    -- Notes analysis
    CASE 
        WHEN r.notes LIKE '%reissue%' OR r.notes LIKE '%repress%' OR 
             r.notes LIKE '%re-issue%' OR r.notes LIKE '%re-press%' OR
             r.notes LIKE '%remaster%' OR r.notes LIKE '%anniversary%'
        THEN -1 ELSE 0 
    END as notes_score,
    
    -- Format descriptions
    CASE 
        WHEN JSON_EXTRACT(r.formats, '$[0].descriptions') LIKE '%Reissue%' OR
             JSON_EXTRACT(r.formats, '$[0].descriptions') LIKE '%Repress%'
        THEN -1 ELSE 0
    END as format_score,
    
    -- Is it the designated main release?
    CASE WHEN r.id = m.main_release_id THEN 2 ELSE 0 END as main_release_score,
    
    -- Calculate total originality score
    (CASE WHEN r.year = m.year THEN 1 ELSE 0 END) +
    (CASE WHEN r.id = m.main_release_id THEN 2 ELSE 0 END) +
    (CASE 
        WHEN r.notes LIKE '%reissue%' OR r.notes LIKE '%repress%' OR 
             r.notes LIKE '%re-issue%' OR r.notes LIKE '%re-press%' OR
             r.notes LIKE '%remaster%' OR r.notes LIKE '%anniversary%'
        THEN -1 ELSE 0 
    END) +
    (CASE 
        WHEN JSON_EXTRACT(r.formats, '$[0].descriptions') LIKE '%Reissue%' OR
             JSON_EXTRACT(r.formats, '$[0].descriptions') LIKE '%Repress%'
        THEN -1 ELSE 0
    END) as originality_score,
    
    -- Final assessment
    CASE 
        WHEN r.id = m.main_release_id THEN 'Main Release (Likely Original)'
        WHEN r.year = m.year AND 
             (r.notes NOT LIKE '%reissue%' AND r.notes NOT LIKE '%repress%' AND 
              r.notes NOT LIKE '%re-issue%' AND r.notes NOT LIKE '%re-press%' AND
              r.notes NOT LIKE '%remaster%' AND r.notes NOT LIKE '%anniversary%' OR r.notes IS NULL)
        THEN 'Likely Original Press'
        WHEN r.year > m.year THEN 'Later Press/Reissue'
        ELSE 'Uncertain'
    END as assessment

FROM releases r
INNER JOIN masters m ON r.master_id = m.id
INNER JOIN user_collection uc ON r.id = uc.release_id
WHERE m.year IS NOT NULL AND r.year IS NOT NULL
ORDER BY originality_score DESC, r.year ASC;
```

### Advantages
- ✅ Multi-factor analysis for more accurate assessment
- ✅ Scoring system allows for nuanced evaluation
- ✅ Considers notes, formats, and main release designation
- ✅ Provides transparency in decision-making process
- ✅ Handles edge cases better than simple year matching

### Disadvantages
- ❌ More complex query with higher computational cost
- ❌ Still requires manual review of uncertain cases
- ❌ Keyword matching may miss non-English terms
- ❌ Scoring weights are arbitrary and may need adjustment

---

## 5. Complete Collection Analysis

### Purpose
Analyze all releases in collection to understand why some are excluded from originality queries.

### SQL Query
```sql
SELECT 
    r.id,
    r.title,
    r.year as release_year,
    m.year as master_year,
    r.country,
    
    CASE 
        WHEN m.id IS NULL THEN 'No Master Data'
        WHEN r.year IS NULL OR m.year IS NULL THEN 'Missing Year Data'
        WHEN r.id = m.main_release_id THEN 'Main Release (Likely Original)'
        WHEN r.year = m.year THEN 'Same Year (Possible Original)'
        WHEN r.year < m.year THEN 'Pre-Master (Check Data Quality)'
        WHEN r.year > m.year THEN 'Later Press/Reissue'
        ELSE 'Unknown Status'
    END as status,
    
    -- Show exclusion reason from strict query
    CASE 
        WHEN m.id IS NULL THEN 'No master relationship'
        WHEN r.year IS NULL THEN 'Missing release year'
        WHEN m.year IS NULL THEN 'Missing master year'
        WHEN r.year != m.year THEN 'Different years'
        ELSE 'Included in strict query'
    END as exclusion_reason

FROM user_collection uc
LEFT JOIN releases r ON uc.release_id = r.id
LEFT JOIN masters m ON r.master_id = m.id
ORDER BY status, r.year;
```

### Advantages
- ✅ Shows ALL releases in collection
- ✅ Explains why releases are excluded from other queries
- ✅ Identifies data quality issues
- ✅ Comprehensive overview of collection composition
- ✅ Uses LEFT JOINs to avoid missing data

### Disadvantages
- ❌ Large result set may be overwhelming
- ❌ Less focused analysis
- ❌ Doesn't provide detailed originality scoring
- ❌ May include many uncertain/incomplete records

---

## Usage Recommendations

### For Identifying Likely Original Pressings:
1. Start with **Query #4 (Multi-Factor Analysis)** for comprehensive assessment
2. Use **Query #3 (First Press)** for quick year-based filtering
3. Supplement with **Query #5 (Complete Analysis)** to understand data gaps

### For Specific Artist Research:
- Use **Query #2 (Artist-Specific)** with appropriate artist name

### For Country Analysis:
- Use **Query #1 (Country Information)** for geographic distribution studies

### Data Quality Considerations:
- Missing year data is common in Discogs - don't assume missing = reissue
- Notes and format descriptions are user-contributed and may be inconsistent
- Main release designation in Discogs is editorial and generally reliable
- Country information may be incomplete for older releases

### Performance Notes:
- Queries with subqueries (country determination) may be slower on large datasets
- Consider adding indexes on frequently queried fields if performance becomes an issue
- JSON field queries (formats, notes) may be slower than regular text fields