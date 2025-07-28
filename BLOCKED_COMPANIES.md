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
**Successfully Converted**: 8 companies (10.5%)  
**Blocked/Challenging**: 2 companies (2.6%)  
**Remaining**: 66 companies (86.8%)

**Success Patterns Proven**: 5 distinct scraping methodologies  
**Architecture Validation**: Complete ✅

---

## Next Review Date

**Target**: After converting 20+ working scrapers  
**Rationale**: Build momentum with easier conversions first, then tackle complex cases with proven architecture  
**Priority**: Focus on companies with stable, accessible APIs 