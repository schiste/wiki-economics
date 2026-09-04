import path from "node:path";
import { siteFooter } from "./site-footer.js";

const isDev = process.argv.some(a => a === "preview" || a === "dev");
const distDir = process.env.WIKI_ECON_SITE_DIST_DIR
  ? path.resolve(process.env.WIKI_ECON_SITE_DIST_DIR)
  : "dist";
const sourceDir = process.env.WIKI_ECON_SITE_SOURCE_DIR
  ? path.resolve(process.env.WIKI_ECON_SITE_SOURCE_DIR)
  : "src";
const adminPort = process.env.WIKI_ECON_ADMIN_PORT || "3001";
const adminApiBase = process.env.WIKI_ECON_ADMIN_API_BASE
  ? process.env.WIKI_ECON_ADMIN_API_BASE
  : (isDev ? `http://127.0.0.1:${adminPort}/api` : "/admin-api");

// The admin page is intentionally hidden from the public nav in non-dev
// builds, but the built HTML still exists so an authenticated VPS admin
// server can serve it at /admin. The injected API base switches between the
// local loopback API in dev and the reverse-proxied same-origin path in VPS
// deployments.
const adminApiScript = `<script>
window.__wikiEconAdminApiBase=${JSON.stringify(adminApiBase)};
</script>`;

export default {
  title: "Wiki Economics",
  root: sourceDir,
  output: distDir,
  pager: false,
  // Framework defaults to a Google Fonts stylesheet (Source Serif 4) for
  // headings; we use system fonts everywhere, so drop it to avoid the
  // network fetch.
  globalStylesheets: [],
  footer: siteFooter,
  head: () => `<link rel="stylesheet" href="./style.css">
${adminApiScript}
<script>
(function(){var t=localStorage.getItem("wk-theme");if(t&&t!=="auto"){document.documentElement.setAttribute("data-theme",t);document.documentElement.style.colorScheme=t;}})();
</script>
<script>
document.addEventListener("DOMContentLoaded",function(){
  var sidebar=document.getElementById("observablehq-sidebar");
  if(!sidebar)return;
  var sidebarToggle=document.getElementById("observablehq-sidebar-toggle");
  var center=document.getElementById("observablehq-center");
  var mobileQuery=window.matchMedia("(max-width: 1007px)");
  var masthead=document.createElement("header");
  masthead.className="mobile-masthead";
  var mastheadContext=document.createElement("div");
  mastheadContext.className="mobile-masthead-context";
  var mastheadBrand=document.createElement("a");
  mastheadBrand.className="mobile-masthead-brand";
  mastheadBrand.href="/";
  mastheadBrand.textContent="Wiki Economics";
  var activeLinks=sidebar.querySelectorAll(".observablehq-link-active a");
  var mastheadPage=document.createElement("span");
  mastheadPage.className="mobile-masthead-page";
  mastheadPage.textContent=activeLinks.length?activeLinks[activeLinks.length-1].textContent:"Portfolio";
  mastheadContext.appendChild(mastheadBrand);
  mastheadContext.appendChild(mastheadPage);
  var menuButton=document.createElement("button");
  menuButton.type="button";
  menuButton.className="mobile-menu-button";
  menuButton.setAttribute("aria-controls","observablehq-sidebar");
  menuButton.setAttribute("aria-expanded","false");
  menuButton.setAttribute("aria-label","Open navigation");
  var menuIcon=document.createElement("span");
  menuIcon.className="mobile-menu-icon";
  menuIcon.setAttribute("aria-hidden","true");
  for(var menuLineIndex=0;menuLineIndex<3;menuLineIndex++){
    var menuLine=document.createElement("span");
    menuLine.className="mobile-menu-line";
    menuIcon.appendChild(menuLine);
  }
  var menuLabel=document.createElement("span");
  menuLabel.className="mobile-menu-label";
  menuLabel.textContent="Menu";
  menuButton.appendChild(menuIcon);
  menuButton.appendChild(menuLabel);
  masthead.appendChild(mastheadContext);
  masthead.appendChild(menuButton);
  document.body.insertBefore(masthead,center);
  var navToolbar=document.createElement("div");
  navToolbar.className="mobile-nav-toolbar";
  var navTitle=document.createElement("strong");
  navTitle.textContent="Navigation";
  var closeButton=document.createElement("button");
  closeButton.type="button";
  closeButton.className="mobile-nav-close";
  closeButton.setAttribute("aria-label","Close navigation");
  var closeIcon=document.createElement("span");
  closeIcon.setAttribute("aria-hidden","true");
  closeIcon.textContent="\u00D7";
  closeButton.appendChild(closeIcon);
  closeButton.appendChild(document.createTextNode("Close"));
  navToolbar.appendChild(navTitle);
  navToolbar.appendChild(closeButton);
  sidebar.insertBefore(navToolbar,sidebar.firstElementChild.nextSibling);
  sidebar.setAttribute("aria-label","Primary navigation");
  var scrollLockY=0;
  var pageLocked=false;
  var navigationPending=false;
  var previousOpen=false;
  function isMobileNavOpen(){
    return mobileQuery.matches&&sidebarToggle.checked&&!sidebarToggle.indeterminate;
  }
  function setPageLocked(locked){
    if(locked===pageLocked)return;
    pageLocked=locked;
    if(locked){
      scrollLockY=window.scrollY;
      document.documentElement.classList.add("mobile-nav-open");
      document.body.style.position="fixed";
      document.body.style.top="-"+scrollLockY+"px";
      document.body.style.left="0";
      document.body.style.right="0";
      document.body.style.width="100%";
    }else{
      document.documentElement.classList.remove("mobile-nav-open");
      document.body.style.removeProperty("position");
      document.body.style.removeProperty("top");
      document.body.style.removeProperty("left");
      document.body.style.removeProperty("right");
      document.body.style.removeProperty("width");
      window.scrollTo(0,scrollLockY);
    }
  }
  function syncMobileNavigation(){
    var open=isMobileNavOpen();
    menuButton.setAttribute("aria-expanded",open?"true":"false");
    menuButton.setAttribute("aria-label",open?"Close navigation":"Open navigation");
    menuLabel.textContent=open?"Close":"Menu";
    sidebar.setAttribute("aria-hidden",mobileQuery.matches&&!open?"true":"false");
    sidebarToggle.tabIndex=mobileQuery.matches?-1:0;
    setPageLocked(open);
    if(open&&!previousOpen){
      window.requestAnimationFrame(function(){closeButton.focus({preventScroll:true});});
    }else if(!open&&previousOpen&&!navigationPending){
      window.requestAnimationFrame(function(){menuButton.focus({preventScroll:true});});
    }
    previousOpen=open;
    navigationPending=false;
  }
  function scheduleMobileNavigationSync(){
    window.requestAnimationFrame(syncMobileNavigation);
  }
  function toggleMobileNavigation(){
    if(sidebarToggle)sidebarToggle.click();
    scheduleMobileNavigationSync();
  }
  menuButton.addEventListener("click",toggleMobileNavigation);
  closeButton.addEventListener("click",toggleMobileNavigation);
  sidebarToggle.addEventListener("click",scheduleMobileNavigationSync);
  sidebarToggle.addEventListener("change",scheduleMobileNavigationSync);
  mobileQuery.addEventListener("change",scheduleMobileNavigationSync);
  sidebar.addEventListener("click",function(event){
    if(!mobileQuery.matches||!isMobileNavOpen()||!event.target.closest("a"))return;
    navigationPending=true;
    toggleMobileNavigation();
  });
  sidebar.addEventListener("keydown",function(event){
    if(event.key!=="Tab"||!isMobileNavOpen())return;
    var focusable=Array.from(sidebar.querySelectorAll("a[href],button:not([disabled]),summary,[tabindex]:not([tabindex='-1'])"));
    if(focusable.length===0)return;
    var first=focusable[0];
    var last=focusable[focusable.length-1];
    if(event.shiftKey&&document.activeElement===first){
      event.preventDefault();
      last.focus();
    }else if(!event.shiftKey&&document.activeElement===last){
      event.preventDefault();
      first.focus();
    }
  });
  syncMobileNavigation();
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
  var adminLink=document.createElement("a");
  adminLink.className="sidebar-admin-link";
  adminLink.href="/admin";
  adminLink.title="Admin";
  adminLink.textContent="\\u2699 Admin";
  var collapse=document.createElement("label");
  collapse.className="sidebar-collapse-btn";
  collapse.setAttribute("for","observablehq-sidebar-toggle");
  collapse.title="Collapse sidebar";
  collapse.textContent="\\u25C2 Collapse";
  bottom.appendChild(themeDiv);
  bottom.appendChild(adminLink);
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
    { name: "Portfolio", path: "/" },
    {
      name: "Indicators",
      pages: [
        { name: "Edit Distribution", path: "/inequality" },
        { name: "Edit Variation", path: "/edit-variation" },
        { name: "Community", path: "/labor" },
        { name: "Content Production", path: "/gdp" },
        { name: "Patrol", path: "/patrol" },
      ],
    },
    {
      name: "Staging",
      pages: [
        { name: "Account Creation", path: "/account-creations" },
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
