# Blocked/Challenging Companies

Companies that need further investigation or have technical challenges preventing immediate conversion.

## Status Categories

- **🚫 BLOCKED**: Anti-bot protection or technical barriers
- **🔄 CHANGED**: Website structure changed since notebook creation
- **🔍 INVESTIGATE**: Complex patterns requiring deeper analysis
- **⏸️ DELAYED**: Working but requires significant time investment

---

## Current Blocked Companies

### 🚫 Starbucks (Complex ZIP Iteration)
- **Issue**: Anti-bot protection on store locator API
- **Pattern**: ZIP code iteration with 1,500 ZIP codes
- **Status**: Code complete but blocked by bot detection
- **Expected**: ~15,000 locations
- **Action Needed**: Investigate headers, rate limiting, or alternative API
- **Priority**: High (large dataset)

### 🔄 Hmart (Website Modernization)
- **Issue**: Website migrated from static HTML to React/VTEX platform
- **Pattern**: Was embedded JSON, now needs API discovery
- **Status**: Original embedded JSON approach obsolete
- **Expected**: ~88 locations
- **Action Needed**: Reverse engineer new API or find alternative endpoint
- **Priority**: Medium (moderate dataset, investigation time)

### 🔄 Menchies (Website Modernization)  
- **Issue**: Store locator migrated to dynamic JavaScript/WordPress integration
- **Pattern**: Was HTML parsing (`div.loc-info`), now dynamic loading
- **Status**: Original HTML structure no longer exists
- **Expected**: ~30 locations
- **Action Needed**: Investigate WordPress API or Google Maps integration
- **Priority**: Low (small dataset)

### 🔄 TCBY (API Deprecation)
- **Issue**: State-by-state API endpoints no longer exist
- **Pattern**: Was state iteration (`/api/geo/{state}/`), now 404 errors
- **Status**: Original API completely removed
- **Expected**: ~50 locations
- **Action Needed**: Find new store locator or API endpoints
- **Priority**: Low (small dataset)

### 🔄 Shipley Donuts (Next.js Migration)
- **Issue**: Website migrated to Next.js/React with server-side rendering
- **Pattern**: Was inline JavaScript JSON (`locations_meta`), now SSR
- **Status**: Original embedded JSON pattern no longer works
- **Expected**: ~320 locations
- **Action Needed**: Investigate Next.js API routes or client-side data loading
- **Priority**: Medium (moderate dataset)

### 🔄 Hollister (API Modernization)
- **Issue**: Store locator API endpoints changed or removed
- **Pattern**: Was ZIP iteration with search radius API
- **Status**: Original API returns 400 Bad Request errors
- **Expected**: ~104 locations
- **Action Needed**: Find new store locator API or alternative endpoints
- **Priority**: Low (small dataset)

### 🔄 Crumbl Cookies (Next.js Build ID Change)
- **Issue**: Next.js static data API requires dynamic build ID
- **Pattern**: Was Next.js static data endpoint with hardcoded build ID
- **Status**: Build ID changes with each deployment, causing 404 errors
- **Expected**: ~610 locations
- **Action Needed**: Dynamic build ID extraction or alternative API discovery
- **Priority**: Medium (significant dataset)

### 🚫 Wendy's (Anti-bot Protection)
- **Issue**: API returns 403 Forbidden errors with rapid ZIP code iteration
- **Pattern**: ZIP code iteration with location search API
- **Status**: Worked in original notebook (6,160 locations), now blocked by bot detection
- **Expected**: ~6,160 locations (based on original notebook results)
- **Action Needed**: Investigate slower rate limiting, proxy rotation, or headers modification
- **Priority**: High (large dataset, originally successful)

---

## Investigation Notes

### Starbucks Technical Details
- **Original API**: `https://www.starbucks.com/bff/locations`
- **Parameters**: ZIP code iteration with radius search
- **Challenge**: Returns 403 Forbidden with standard headers
- **Potential Solutions**: 
  - Enhanced header rotation
  - Proxy rotation
  - Alternative store locator endpoint
  - Official API partnership

### Hmart Technical Details
- **Original Pattern**: Extract from `<script xml="space">` → `jsonLocations: {...}`
- **Current State**: Modern React SPA with dynamic loading
- **URL Changed**: `/ourstores` → `/stores` (301 redirect)
- **Challenge**: Data likely loaded via GraphQL/REST API calls
- **Potential Solutions**:
  - Network tab analysis for API endpoints
  - Check for GraphQL queries
  - Mobile app API reverse engineering

---

## Success Rate Tracking

**Total Target**: 76 companies  
**Successfully Converted**: 14 companies (18.4%)  
**Blocked/Challenging**: 8 companies (10.5%)  
**Remaining**: 54 companies (71.1%)

**Success Patterns Proven**: 7 distinct scraping methodologies  
**Architecture Validation**: Complete ✅  
**Total Locations Processed**: 3,086+ locations

---

## Next Review Date

**Target**: After converting 20+ working scrapers  
**Rationale**: Build momentum with easier conversions first, then tackle complex cases with proven architecture  
**Priority**: Focus on companies with stable, accessible APIs 