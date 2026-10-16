package xray

import (
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/features/routing"
)

// Ядро 78 (АВТО 4.1, шаг 2.6; ADR-0001 §13): состояние и проверка маршрута
// запущенного ядра. Логика и форматы JSON — в форке (app/router,
// strategy_vupen_api.go); ядро не запущено — ответ с active=false / verdict unknown.

func vupenRouter() *router.Router {
	s := runningCore()
	if s == nil {
		return nil
	}
	if r, ok := s.GetFeature(routing.RouterType()).(*router.Router); ok {
		return r
	}
	// Ядро без правил маршрутизации (свой маршрутизатор не создан): АВТО 4.1 нет.
	return &router.Router{}
}

// VupenAutoState — JSON состояния АВТО 4.1.
func VupenAutoState() string { return vupenRouter().VupenAutoState() }

// VupenCheckRoute — JSON-запрос check | heal | switch (ядро 79), JSON-ответ.
func VupenCheckRoute(req string) string { return vupenRouter().VupenCheckRoute(req) }
