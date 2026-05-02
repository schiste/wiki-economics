import path from "node:path";

const isDev = process.argv.some(a => a === "preview" || a === "dev");
const distDir = process.env.WIKI_ECON_SITE_DIST_DIR
  ? path.resolve(process.env.WIKI_ECON_SITE_DIST_DIR)
  : "dist";
const adminPort = process.env.WIKI_ECON_ADMIN_PORT || "3001";

// The admin API base URL is only injected into the page head in dev/preview
// builds so production HTML never advertises the local-only admin port. The
// admin page itself is also conditionally added to the nav in dev mode below.
const adminApiScript = isDev
  ? `<script>
window.__wikiEconAdminApiBase=(function(){
  var proto=window.location&&window.location.protocol?window.location.protocol:"http:";
  var host=window.location&&window.location.hostname?window.location.hostname:"127.0.0.1";
  return proto+"//"+host+":${adminPort}";
})();
</script>`
  : "";

export default {
  title: "Wikipedia Economics",
  root: "src",
  output: distDir,
  pager: false,
  head: `<link rel="stylesheet" href="./style.css">
${adminApiScript}
<script>
(function(){var t=localStorage.getItem("wk-theme");if(t&&t!=="auto"){document.documentElement.setAttribute("data-theme",t);document.documentElement.style.colorScheme=t;}})();
</script>
<script>
document.addEventListener("DOMContentLoaded",function(){
  var sidebar=document.getElementById("observablehq-sidebar");
  if(!sidebar)return;
  var bottom=document.createElement("div");
  bottom.className="sidebar-bottom";
  var themeDiv=document.createElement("div");
  themeDiv.className="sidebar-theme";
  var themes=[{v:"light",l:"\\u2600 Light"},{v:"auto",l:"\\u25D0 Auto"},{v:"dark",l:"\\u263E Dark"}];
  var current=localStorage.getItem("wk-theme")||"auto";
  function applyTheme(theme){
    var h=document.documentElement;
    if(theme==="auto"){h.removeAttribute("data-theme");h.style.removeProperty("color-scheme");}
    else{h.setAttribute("data-theme",theme);h.style.colorScheme=theme;}
  }
  themes.forEach(function(t){
    var btn=document.createElement("button");
    btn.className="theme-btn"+(t.v===current?" active":"");
    btn.setAttribute("data-theme-value",t.v);
    btn.title=t.v.charAt(0).toUpperCase()+t.v.slice(1);
    btn.textContent=t.l;
    btn.addEventListener("click",function(){
      applyTheme(t.v);
      localStorage.setItem("wk-theme",t.v);
      themeDiv.querySelectorAll(".theme-btn").forEach(function(b){
        b.classList.toggle("active",b.getAttribute("data-theme-value")===t.v);
      });
    });
    themeDiv.appendChild(btn);
  });
  var collapse=document.createElement("label");
  collapse.className="sidebar-collapse-btn";
  collapse.setAttribute("for","observablehq-sidebar-toggle");
  collapse.title="Collapse sidebar";
  collapse.textContent="\\u25C2 Collapse";
  bottom.appendChild(themeDiv);
  bottom.appendChild(collapse);
  sidebar.appendChild(bottom);
  function addFilterToggle(desc){
    if(desc.querySelector(".filters-toggle"))return;
    var btn=document.createElement("button");
    btn.className="filters-toggle";
    btn.textContent="\\u25BE";
    btn.title="Collapse filters";
    btn.addEventListener("click",function(){
      var bar=desc.closest(".filters-bar");
      bar.classList.toggle("filters-collapsed");
      var c=bar.classList.contains("filters-collapsed");
      btn.textContent=c?"\\u25B8":"\\u25BE";
      btn.title=c?"Expand filters":"Collapse filters";
    });
    desc.appendChild(btn);
  }
  document.querySelectorAll(".filter-desc").forEach(addFilterToggle);
  new MutationObserver(function(){
    document.querySelectorAll(".filter-desc").forEach(addFilterToggle);
  }).observe(document.body,{childList:true,subtree:true});
});
</script>`,
  pages: [
    {
      name: "Indicators",
      pages: [
        { name: "Edit Distribution", path: "/inequality" },
        { name: "Community", path: "/labor" },
        { name: "Content Production", path: "/gdp" },
        { name: "Patrol", path: "/patrol" },
      ],
    },
    {
      name: "Staging",
      pages: [
        { name: "Business Health", path: "/business" },
      ],
    },
    ...(isDev ? [{
      name: "System",
      pages: [
        { name: "Admin", path: "/admin" },
      ],
    }] : []),
  ],
};
