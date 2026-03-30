
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="66181cc0-7551-5615-a793-ccea54c58720")}catch(e){}}();
const c=(e,r)=>{switch(r.type){case"UPDATE_REGION_FILTER_OPTION":return r.payload;case"RESET":return r.payload}},t={INITIAL:6,NEXT_MORE:11,MAX:9999},u=(e,r)=>{switch(r.type){case"MORE":return e===t.INITIAL?t.MAX:e;case"FEWER":return t.INITIAL;case"RESET":return t.INITIAL;default:return e}},s=3,R={kr:e=>{const{region:r,parentRegion:n,childrenRegions:o,siblingRegions:i}=e;return r.depth<s?{root:r,children:o}:{root:n,children:i||[]}},jp:e=>{const{region:r,parentRegion:n,childrenRegions:o,siblingRegions:i}=e;return r.depth<s?{root:r,children:o}:r.depth===s?{root:n,children:i??[]}:{root:n,children:[r]}}};function I(e,r,n){return e.id===n?-1:r.id===n?1:0}export{t as R,c as a,R as b,I as p,u as r};
//# sourceMappingURL=regionFilter-Dm_AgYZN.js.map

//# debugId=66181cc0-7551-5615-a793-ccea54c58720
