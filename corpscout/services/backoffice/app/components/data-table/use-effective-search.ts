import { useLocation, useNavigation } from "react-router";

/**
 * Search params of the effective location: the pending navigation when one
 * exists, otherwise the committed location. Rapid successive filter toggles
 * must each build on the previous (still-pending) URL, or later clicks
 * silently drop earlier selections.
 */
export function useEffectiveSearchParams(): URLSearchParams {
  const navigation = useNavigation();
  const location = useLocation();
  return new URLSearchParams(
    navigation.location ? navigation.location.search : location.search,
  );
}
