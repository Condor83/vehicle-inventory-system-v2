"use strict";
var SmartpathTracker = (function () {
    function SmartpathTracker() {
        var _this = this;
        document.addEventListener('digital-garage-loaded', function () {
            if (window.DGDataHub && window.sd) {
                _this.BuildLeadEventListeners();
                _this.BuildMSTCSearchEventListeners();
                _this.BuildMSTCCtaEventListeners();
            }
        });
    }
    SmartpathTracker.prototype.BuildLeadEventListeners = function () {
        document.addEventListener('do-leads-submission-error', function (e) {
            if (window.sdDataLayerFormType === "Get a Quote" || window.sdDataLayerFormType === "General Contact") {
                window.sdDataLayer.events = "formError";
                window.sdDataLayer.contentSection += " submission";
                window.sd('send');
            }
        });
    };
    SmartpathTracker.prototype.BuildMSTCSearchEventListeners = function () {
        var smartPathElements = document.querySelectorAll('a[href^="/smart-path"], a[href^="/monogram"]');
        smartPathElements === null || smartPathElements === void 0 ? void 0 : smartPathElements.forEach(function (smartPathElement) {
            smartPathElement.addEventListener('click', function () {
                window.sdDataLayer = {
                    linkType: 'MST-C',
                    gxpApp: 'smartpath',
                    ctaText: smartPathElement.textContent,
                    toolName: 'MST-C',
                    events: 'linkClick'
                };
                window.sd('send');
            });
        });
    };
    SmartpathTracker.prototype.BuildMSTCCtaEventListeners = function () {
        var smartPathElements = document.querySelectorAll('a[href*="//smartpath"], a[href*="//monogram"]');
        smartPathElements === null || smartPathElements === void 0 ? void 0 : smartPathElements.forEach(function (smartPathElement) {
            smartPathElement.addEventListener('click', function () {
                var vehicleInfo = smartPathElement.closest('.row.srpVehicle.hasVehicleInfo');
                var vehicleObject = {};
                if (vehicleInfo) {
                    vehicleObject = {
                        status: vehicleInfo.dataset.vehicletype,
                        year: vehicleInfo.dataset.year,
                        make: vehicleInfo.dataset.make,
                        model: vehicleInfo.dataset.model,
                        trim: vehicleInfo.dataset.trim,
                        engine: vehicleInfo.dataset.engine,
                        transmission: vehicleInfo.dataset.trans,
                        drivetrain: vehicleInfo.dataset.drivetrain,
                        interiorColor: vehicleInfo.dataset.intcolor,
                        exteriorColor: vehicleInfo.dataset.extcolor,
                        vin: vehicleInfo.dataset.vin,
                        msrp: vehicleInfo.dataset.msrp,
                        displayedPrice: vehicleInfo.dataset.price,
                        fuelType: vehicleInfo.dataset.fueltype,
                    };
                }
                else if (window.sdDataLayer.formVehicle) {
                    vehicleObject = window.sdDataLayer.formVehicle;
                }
                else if (window.sdDataLayer.vehicleDetails) {
                    vehicleObject = window.sdDataLayer.vehicleDetails;
                }
                window.sdDataLayer = {
                    linkType: 'MST-C',
                    gxpApp: 'smartpath',
                    ctaText: smartPathElement.textContent,
                    toolName: 'MST-C',
                    vehicleDetails: vehicleObject,
                    events: 'linkClick'
                };
                window.sd('send');
            });
        });
    };
    return SmartpathTracker;
}());
if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', function () { return new SmartpathTracker(); });
}
else {
    new SmartpathTracker();
}
